"""遥测分析核心引擎。

职责：
- 批量接收遥测样本，处理乱序、丢包（序列号缺口）与维护窗口内去重；
- 按滑动窗口 + 可热更新阈值规则评估，产生分级告警（info/warning/critical）；
- 配置版本切换前后的窗口段分别评估、分别标注；
- 时间基准无法确认的样本进入待审队列，不参与统计；
- 通信抖动（缺口/重复/迟到）只记数据质量事件，不与姿态类数值告警混淆；
- 告警聚合：同一 (测点, 等级, 配置版本) 的未确认告警持续累积证据，避免告警风暴。
"""
from __future__ import annotations

import fnmatch
import threading
import time
import uuid
from bisect import insort
from dataclasses import dataclass, field
from typing import Any, Callable, Optional

from .models import (
    AlertState,
    BatchResult,
    IngestBatchIn,
    Level,
    SampleIn,
    SampleResult,
    iso,
)
from .rules import RuleSet, parse_ruleset
from .storage import Storage

MAX_TRIGGER_SAMPLES_PER_ALERT = 1000
MAX_OPEN_GAPS_PER_POINT = 10000


@dataclass
class EngineConfig:
    dedup_window_sec: float = 3600.0        # 维护窗口：窗口内重复样本不重复计数
    max_future_skew_sec: float = 300.0      # 超过该未来偏差 → 时间基准不可确认
    max_past_skew_sec: float = 7 * 86400.0  # 超过该历史跨度 → 时间基准不可确认


@dataclass
class PointState:
    point_id: str
    window: list[tuple[float, int, float, str]] = field(default_factory=list)  # (ts, seq, value, cfg) 按 (ts, seq) 有序
    high_seq: Optional[int] = None
    current_config: Optional[str] = None
    open_alerts: dict[tuple[str, str], str] = field(default_factory=dict)       # (level, cfg) -> alert_id
    last_resolved: dict[tuple[str, str], float] = field(default_factory=dict)   # 冷却计时
    open_gaps: set[int] = field(default_factory=set)


def _new_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


class TelemetryCore:
    def __init__(
        self,
        storage: Storage,
        ruleset: RuleSet,
        config: Optional[EngineConfig] = None,
        clock: Callable[[], float] = time.time,
    ):
        self.storage = storage
        self.rules = ruleset
        self.config = config or EngineConfig()
        self.clock = clock
        self._lock = threading.RLock()
        self._points: dict[str, PointState] = {}
        self._alert_seqs: dict[str, set[int]] = {}   # alert_id -> 已计入的样本 seq
        self._alert_meta: dict[str, tuple[str, str, str]] = {}  # alert_id -> (point, level, cfg)
        self._recover()

    # ------------------------------------------------------------------
    # 重启恢复：未确认告警与序列号水位不丢失
    # ------------------------------------------------------------------

    def _recover(self) -> None:
        for point_id, high in self.storage.all_watermarks().items():
            self._point(point_id).high_seq = high
        for a in self.storage.open_alerts():
            st = self._point(a["point_id"])
            key = (a["level"], a["config_version"])
            st.open_alerts[key] = a["alert_id"]
            seqs = {s["seq"] for s in self.storage.get_alert_samples(a["alert_id"])}
            self._alert_seqs[a["alert_id"]] = seqs
            self._alert_meta[a["alert_id"]] = (a["point_id"], a["level"], a["config_version"])

    def _point(self, point_id: str) -> PointState:
        st = self._points.get(point_id)
        if st is None:
            st = self._points[point_id] = PointState(point_id=point_id)
        return st

    # ------------------------------------------------------------------
    # 批量接收
    # ------------------------------------------------------------------

    def ingest_batch(self, batch: IngestBatchIn) -> BatchResult:
        now = self.clock()
        batch_id = _new_id("b")
        results: list[SampleResult] = []
        counts = {"accepted": 0, "duplicate": 0, "late": 0, "review": 0, "rejected": 0}
        touched: set[str] = set()
        with self._lock:
            for s in batch.samples:
                r = self._process(s, now, touched)
                results.append(r)
                counts["accepted" if r.status == "accepted" else
                        "duplicate" if r.status == "duplicate" else
                        "late" if r.status == "late" else
                        "review" if r.status == "review" else "rejected"] += 1
            for pid in touched:
                self._evaluate(pid, now)
            summary = {
                "total": len(results),
                **{k: v for k, v in counts.items()},
                "points": sorted({r.point_id for r in results}),
            }
            self.storage.batch_save(batch_id, batch.source, now, summary)
        return BatchResult(
            batch_id=batch_id,
            received_at=iso(now),
            total=len(results),
            accepted=counts["accepted"],
            duplicates=counts["duplicate"],
            late=counts["late"],
            review=counts["review"],
            rejected=counts["rejected"],
            results=results,
        )

    def _review(self, s: SampleIn, reason: str, now: float) -> SampleResult:
        rid = self.storage.review_add(s.model_dump(), reason, now)
        self.storage.add_event("review_enqueue", s.point_id,
                               {"seq": s.seq, "reason": reason, "review_id": rid}, now)
        return SampleResult(point_id=s.point_id, seq=s.seq, status="review",
                            detail=f"时间基准无法确认({reason})，已进入待审队列 #{rid}",
                            ts=iso(s.epoch()))

    def _process(self, s: SampleIn, now: float, touched: set[str]) -> SampleResult:
        # 1) 时间基准确认：无法确认 → 待审队列，绝不进入统计
        epoch = s.epoch()
        if epoch is None:
            return self._review(s, "timestamp_missing_or_unparseable", now)
        if not s.time_confirmed():
            return self._review(s, "time_quality_unconfirmed", now)
        if epoch > now + self.config.max_future_skew_sec:
            return self._review(s, "future_timestamp", now)
        if epoch < now - self.config.max_past_skew_sec:
            return self._review(s, "stale_timestamp", now)

        st = self._point(s.point_id)

        # 2) 维护窗口内去重：重复样本不重复计数
        if not self.storage.dedup_insert_if_absent(s.point_id, s.seq, now):
            self.storage.add_event("duplicate", s.point_id, {"seq": s.seq, "ts": epoch}, now)
            return SampleResult(point_id=s.point_id, seq=s.seq, status="duplicate",
                                detail="维护窗口内重复样本，未重复计数", ts=iso(epoch))

        # 3) 序列号水位 / 乱序 / 丢包缺口
        out_of_order = False
        if st.high_seq is None:
            st.high_seq = s.seq
            self.storage.set_watermark(s.point_id, s.seq, now)
        elif s.seq > st.high_seq:
            if s.seq > st.high_seq + 1:
                missing = list(range(st.high_seq + 1, min(s.seq, st.high_seq + 1 + MAX_OPEN_GAPS_PER_POINT)))
                st.open_gaps.update(missing)
                if len(st.open_gaps) > MAX_OPEN_GAPS_PER_POINT:
                    st.open_gaps.clear()
                self.storage.add_event(
                    "seq_gap", s.point_id,
                    {"from_seq": st.high_seq + 1, "to_seq": s.seq - 1,
                     "gap_len": s.seq - st.high_seq - 1}, now)
            st.high_seq = s.seq
            self.storage.set_watermark(s.point_id, s.seq, now)
        elif s.seq < st.high_seq:
            out_of_order = True
            if s.seq in st.open_gaps:
                st.open_gaps.discard(s.seq)
                self.storage.add_event("seq_gap_filled", s.point_id, {"seq": s.seq}, now)

        # 4) 超出滑动窗口的历史样本：记迟到，不进入统计
        window_sec = self.rules.window_sec(s.point_id)
        newest = st.window[-1][0] if st.window else None
        if newest is not None and epoch < newest - window_sec:
            self.storage.add_event("late_sample", s.point_id,
                                   {"seq": s.seq, "ts": epoch, "window_sec": window_sec}, now)
            return SampleResult(point_id=s.point_id, seq=s.seq, status="late",
                                detail="样本时间已滑出维护窗口，仅留痕不参与统计",
                                out_of_order=True, ts=iso(epoch))

        # 5) 进入滑动窗口；配置版本切换留痕
        prev_cfg = st.current_config
        insort(st.window, (epoch, s.seq, s.value, s.config_version))
        if not st.window or st.window[-1][0] == epoch:
            st.current_config = s.config_version
        if prev_cfg and prev_cfg != s.config_version and epoch >= (st.window[-1][0] if st.window else epoch):
            self.storage.add_event("config_switch", s.point_id,
                                   {"from": prev_cfg, "to": s.config_version, "ts": epoch}, now)
        self._prune_window(st, window_sec, st.window[-1][0])
        touched.add(s.point_id)
        return SampleResult(point_id=s.point_id, seq=s.seq, status="accepted",
                            detail="", out_of_order=out_of_order, ts=iso(epoch))

    @staticmethod
    def _prune_window(st: PointState, window_sec: float, anchor: float) -> None:
        if not st.window:
            return
        cut = anchor - window_sec
        i = 0
        while i < len(st.window) and st.window[i][0] < cut:
            i += 1
        if i:
            del st.window[:i]

    # ------------------------------------------------------------------
    # 滑动窗口评估：按配置版本分段，分别标注
    # ------------------------------------------------------------------

    def _segments(self, st: PointState) -> dict[str, list[tuple[float, int, float, str]]]:
        segs: dict[str, list[tuple[float, int, float, str]]] = {}
        for entry in st.window:
            segs.setdefault(entry[3], []).append(entry)
        return segs

    def _evaluate(self, point_id: str, now: float) -> None:
        st = self._points.get(point_id)
        if not st or not st.window:
            return
        rule = self.rules.point(point_id)
        if not rule or not rule.levels:
            return
        min_samples = self.rules.min_samples(point_id)
        for cfg, seg in self._segments(st).items():
            if len(seg) < min_samples:
                continue
            for level_name, lr in rule.levels.items():
                violations = []
                prev_val: Optional[float] = None
                for ts, seq, value, _ in seg:
                    if lr.when.matches(value, prev_val):
                        violations.append((ts, seq, value))
                    prev_val = value
                if len(violations) >= lr.min_count:
                    self._raise(point_id, level_name, cfg, seg, violations, now)

    def _raise(self, point_id: str, level: str, cfg: str,
               seg: list[tuple[float, int, float, str]],
               violations: list[tuple[float, int, float]], now: float) -> None:
        st = self._point(point_id)
        key = (level, cfg)
        stats = self._stats(seg, violations)
        stats["window_sec"] = self.rules.window_sec(point_id)
        locator = {
            "point_id": point_id,
            "seq_min": min(v[1] for v in violations),
            "seq_max": max(v[1] for v in violations),
            "ts_first": iso(min(v[0] for v in violations)),
            "ts_last": iso(max(v[0] for v in violations)),
        }
        message = (f"{point_id} {level}: 配置版本 {cfg} 下 {stats['window_sec']:.0f}s 窗口内 "
                   f"{len(violations)}/{stats['segment_samples']} 个样本越限")

        alert_id = st.open_alerts.get(key)
        if alert_id:
            # 聚合到未确认告警：只追加新样本，避免告警风暴
            seen = self._alert_seqs.setdefault(alert_id, set())
            new_samples = [
                {"point_id": point_id, "seq": seq, "ts": ts, "value": val,
                 "config_version": cfg}
                for ts, seq, val in violations if seq not in seen
            ]
            if not new_samples:
                return
            seen.update(s["seq"] for s in new_samples)
            self.storage.add_alert_samples(alert_id, new_samples)
            count = len(seen)
            self.storage.update_alert(alert_id, updated_at=now, violation_count=count,
                                      stats=stats, locator=locator, message=message)
            return

        # 冷却期：刚恢复的同类告警不立即重开
        last = st.last_resolved.get(key)
        if last is not None and now - last < self.rules.cooldown_sec(point_id):
            self.storage.add_event("alert_cooldown", point_id,
                                   {"level": level, "config_version": cfg}, now)
            return

        alert_id = _new_id("al")
        supp = self._match_suppression(point_id, level, now)
        state = AlertState.SUPPRESSED if supp else AlertState.ACTIVE
        record = {
            "alert_id": alert_id, "point_id": point_id, "level": level, "state": state,
            "rule_version": self.rules.version, "config_version": cfg,
            "opened_at": now, "updated_at": now,
            "violation_count": len(violations), "message": message,
            "stats": stats, "locator": locator,
            "suppressed_by": supp["supp_id"] if supp else None,
        }
        samples = [
            {"point_id": point_id, "seq": seq, "ts": ts, "value": val, "config_version": cfg}
            for ts, seq, val in violations[:MAX_TRIGGER_SAMPLES_PER_ALERT]
        ]
        self.storage.insert_alert(record)
        self.storage.add_alert_samples(alert_id, samples)
        st.open_alerts[key] = alert_id
        self._alert_seqs[alert_id] = {s["seq"] for s in samples}
        self._alert_meta[alert_id] = (point_id, level, cfg)
        self.storage.add_event(
            "alert_suppressed" if supp else "alert_opened", point_id,
            {"alert_id": alert_id, "level": level, "config_version": cfg,
             "rule_version": self.rules.version,
             **({"suppression": supp["supp_id"]} if supp else {})}, now)

    @staticmethod
    def _stats(seg: list[tuple[float, int, float, str]],
               violations: list[tuple[float, int, float]]) -> dict[str, Any]:
        values = [e[2] for e in seg]
        return {
            "segment_samples": len(seg),
            "window_span_sec": round(seg[-1][0] - seg[0][0], 6) if len(seg) > 1 else 0.0,
            "min": min(values), "max": max(values),
            "mean": round(sum(values) / len(values), 6),
            "violations": len(violations),
        }

    # ------------------------------------------------------------------
    # 周期驱动：自动恢复、窗口与去重维护
    # ------------------------------------------------------------------

    def tick(self, now: Optional[float] = None) -> None:
        now = self.clock() if now is None else now
        with self._lock:
            for point_id, st in list(self._points.items()):
                window_sec = self.rules.window_sec(point_id)
                # tick 用墙上时钟推进窗口：数据停流后窗口内的违规也会随时间滑出
                anchor = max(st.window[-1][0], now) if st.window else now
                self._prune_window(st, window_sec, anchor)
                segs = self._segments(st)
                rule = self.rules.point(point_id)
                for (level, cfg), alert_id in list(st.open_alerts.items()):
                    resolve_sec = self.rules.resolve_sec(point_id)
                    if now - self._alert_updated_at(alert_id) < resolve_sec:
                        continue
                    remaining = 0
                    lr = rule.levels.get(level) if rule else None
                    if lr:
                        seg = segs.get(cfg, [])
                        prev_val: Optional[float] = None
                        for ts, seq, value, _ in seg:
                            if lr.when.matches(value, prev_val):
                                remaining += 1
                            prev_val = value
                    if remaining == 0:
                        self._resolve(alert_id, st, (level, cfg), now)
            self.storage.dedup_prune(now - self.config.dedup_window_sec)

    def _alert_updated_at(self, alert_id: str) -> float:
        a = self.storage.get_alert(alert_id)
        return a["updated_at"] if a else 0.0

    def _resolve(self, alert_id: str, st: PointState, key: tuple[str, str], now: float) -> None:
        self.storage.update_alert(alert_id, state=AlertState.RESOLVED, resolved_at=now,
                                  updated_at=now)
        st.open_alerts.pop(key, None)
        st.last_resolved[key] = now
        self.storage.add_event("alert_resolved", st.point_id,
                               {"alert_id": alert_id, "level": key[0],
                                "config_version": key[1]}, now)

    # ------------------------------------------------------------------
    # 告警确认 / 抑制
    # ------------------------------------------------------------------

    def ack_alert(self, alert_id: str, operator: str, note: str = "") -> Optional[dict[str, Any]]:
        now = self.clock()
        with self._lock:
            a = self.storage.get_alert(alert_id)
            if not a:
                return None
            if a["state"] not in (AlertState.ACTIVE, AlertState.SUPPRESSED):
                return a
            st = self._point(a["point_id"])
            key = (a["level"], a["config_version"])
            if st.open_alerts.get(key) == alert_id:
                st.open_alerts.pop(key, None)
                st.last_resolved[key] = now  # 确认后同样进入冷却，防止立即复发
            self.storage.update_alert(alert_id, state=AlertState.ACKNOWLEDGED,
                                      ack_by=operator, ack_at=now, ack_note=note,
                                      updated_at=now)
            self.storage.add_event("alert_ack", a["point_id"],
                                   {"alert_id": alert_id, "operator": operator}, now)
            return self.storage.get_alert(alert_id)

    def add_suppression(self, point_pattern: str, level: Optional[str], starts_at: float,
                        ends_at: float, reason: str, created_by: str) -> dict[str, Any]:
        now = self.clock()
        supp = {"supp_id": _new_id("sup"), "point_pattern": point_pattern,
                "level": level, "starts_at": starts_at, "ends_at": ends_at,
                "reason": reason, "created_by": created_by, "created_at": now}
        with self._lock:
            self.storage.suppression_add(supp)
            # 命中现存未确认告警 → 置为已抑制
            for a in self.storage.query_alerts(state=AlertState.ACTIVE, limit=10000):
                if self._supp_matches(supp, a["point_id"], a["level"], now):
                    st = self._point(a["point_id"])
                    st.open_alerts.pop((a["level"], a["config_version"]), None)
                    self.storage.update_alert(a["alert_id"], state=AlertState.SUPPRESSED,
                                              suppressed_by=supp["supp_id"], updated_at=now)
            self.storage.add_event("suppression_added", None,
                                   {"supp_id": supp["supp_id"], "pattern": point_pattern,
                                    "level": level}, now)
        return supp

    def revoke_suppression(self, supp_id: str) -> bool:
        with self._lock:
            return self.storage.suppression_revoke(supp_id)

    def _match_suppression(self, point_id: str, level: str, now: float) -> Optional[dict[str, Any]]:
        for s in self.storage.suppression_list(include_revoked=False):
            if self._supp_matches(s, point_id, level, now):
                return s
        return None

    @staticmethod
    def _supp_matches(s: dict[str, Any], point_id: str, level: str, now: float) -> bool:
        if not (s["starts_at"] <= now <= s["ends_at"]):
            return False
        if s.get("level") and s["level"] != level:
            return False
        return fnmatch.fnmatchcase(point_id, s["point_pattern"])

    # ------------------------------------------------------------------
    # 规则热更新
    # ------------------------------------------------------------------

    def reload_rules(self, content: str, applied_by: str = "api") -> RuleSet:
        rs = parse_ruleset(content)
        with self._lock:
            self.rules = rs
            self.storage.rules_save(rs.version, content, self.clock(), applied_by)
            self.storage.add_event("rules_reloaded", None,
                                   {"version": rs.version, "applied_by": applied_by},
                                   self.clock())
        return rs

    # ------------------------------------------------------------------
    # 待审队列处置
    # ------------------------------------------------------------------

    def review_resolve(self, rid: int, action: str, operator: str,
                       corrected_ts: Any = None) -> Optional[dict[str, Any]]:
        now = self.clock()
        with self._lock:
            entry = self.storage.review_get(rid)
            if not entry or entry["status"] != "pending":
                return None
            if action == "drop":
                self.storage.review_resolve(rid, "dropped", {"operator": operator}, now)
                return self.storage.review_get(rid)
            if action != "ingest":
                raise ValueError("action 必须是 ingest 或 drop")
            data = dict(entry["sample"])
            if corrected_ts is not None:
                data["ts"] = corrected_ts
            data["time_quality"] = "synced"  # 值班员确认时间基准
            sample = SampleIn.model_validate(data)
            touched: set[str] = set()
            result = self._process(sample, now, touched)
            for pid in touched:
                self._evaluate(pid, now)
            self.storage.review_resolve(
                rid, "ingested",
                {"operator": operator, "corrected_ts": corrected_ts,
                 "ingest_status": result.status}, now)
            return self.storage.review_get(rid)

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------

    def get_alert_detail(self, alert_id: str) -> Optional[dict[str, Any]]:
        a = self.storage.get_alert(alert_id)
        if not a:
            return None
        a = dict(a)
        a["trigger_samples"] = self.storage.get_alert_samples(alert_id)
        return a

    def points_overview(self) -> list[dict[str, Any]]:
        with self._lock:
            out = []
            for pid, st in sorted(self._points.items()):
                out.append({
                    "point_id": pid,
                    "high_seq": st.high_seq,
                    "window_samples": len(st.window),
                    "current_config_version": st.current_config,
                    "open_alerts": len(st.open_alerts),
                    "open_gaps": len(st.open_gaps),
                })
            return out

    def window_snapshot(self, point_id: str) -> Optional[dict[str, Any]]:
        with self._lock:
            st = self._points.get(point_id)
            if not st:
                return None
            return {
                "point_id": point_id,
                "window_sec": self.rules.window_sec(point_id),
                "samples": [
                    {"ts": iso(ts), "seq": seq, "value": val, "config_version": cfg}
                    for ts, seq, val, cfg in st.window
                ],
            }
