"""FastAPI 应用：遥测接收、告警、抑制、待审、历史检索、规则热更新。"""
from __future__ import annotations

import json
import os
import threading
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from .engine import Engine
from .models import AckIn, BatchIn, ResolvePendingIn, RuleSetIn, SuppressionIn
from .storage import Storage
from .util import now_ms, parse_iso, to_iso

DB_PATH = os.environ.get("TELEMETRY_DB", "data/telemetry.db")
RULES_FILE = os.environ.get("TELEMETRY_RULES", "config/rules.json")
RULES_POLL_SEC = float(os.environ.get("TELEMETRY_RULES_POLL", "2"))

storage = Storage(DB_PATH)
engine = Engine(storage)


class RuleFileWatcher(threading.Thread):
    """轮询规则文件 mtime，实现不重启进程的阈值热更新。"""

    def __init__(self, engine: Engine, path: str, interval: float):
        super().__init__(daemon=True, name="rules-watcher")
        self.engine = engine
        self.path = Path(path)
        self.interval = interval
        self._sig: Optional[tuple] = None
        self.last_error: Optional[str] = None
        self._started_once = False

    def _signature(self) -> Optional[tuple]:
        try:
            st = self.path.stat()
            return (st.st_mtime_ns, st.st_size)
        except OSError:
            return None

    def load_once(self, force: bool = False) -> Optional[dict]:
        sig = self._signature()
        if sig is None:
            return None
        if not force and sig == self._sig:
            return None
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            version = str(data["version"])
            rules = data["rules"]
            result = self.engine.put_ruleset(
                version, rules, activate=True,
                note=f"hot-reload from {self.path.name}", actor="rule-watcher",
            )
            self._sig = sig
            self.last_error = None
            return result
        except Exception as exc:  # 坏文件不能搞崩服务，保留现行规则
            self.last_error = f"{type(exc).__name__}: {exc}"
            return None

    def run(self) -> None:
        while True:
            time.sleep(self.interval)
            self.load_once()


watcher = RuleFileWatcher(engine, RULES_FILE, RULES_POLL_SEC)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 启动时：若库中无激活规则而文件存在，先加载
    if engine.active_rule_version is None:
        watcher.load_once(force=True)
    if not watcher._started_once:  # TestClient 会多次进入 lifespan
        watcher._started_once = True
        watcher.start()
    yield


app = FastAPI(title="卫星遥测异常告警服务", version="1.0.0", lifespan=lifespan)


@app.exception_handler(KeyError)
async def keyerror_handler(request: Request, exc: KeyError):
    return JSONResponse(status_code=404, content={"error": "not_found", "detail": str(exc)})


@app.exception_handler(ValueError)
async def valueerror_handler(request: Request, exc: ValueError):
    return JSONResponse(status_code=400, content={"error": "bad_request", "detail": str(exc)})


# ------------------------------------------------------------------ 遥测
@app.post("/api/v1/telemetry/batch", tags=["telemetry"])
def receive_batch(batch: BatchIn):
    """批量接收遥测样本：乱序/丢包统计、去重、滑动窗口分级告警。"""
    if not batch.samples:
        raise HTTPException(400, "samples 不能为空")
    return engine.ingest_batch(batch)


@app.get("/api/v1/telemetry/pending", tags=["telemetry"])
def get_pending(status: str = "open", satellite_id: Optional[str] = None):
    if status not in ("open", "accepted", "rejected"):
        raise HTTPException(400, "status 只能为 open/accepted/rejected")
    return {"items": engine.list_pending(status, satellite_id)}


@app.post("/api/v1/telemetry/pending/{pending_id}/resolve", tags=["telemetry"])
def resolve_pending(pending_id: int, body: ResolvePendingIn):
    """处置时间基准无法确认的样本：accept（可给修正时间）或 reject。"""
    try:
        return engine.resolve_pending(pending_id, body.action, body.corrected_time,
                                      body.note, operator="operator")
    except KeyError:
        raise HTTPException(404, f"待审样本不存在: {pending_id}")


@app.get("/api/v1/streams", tags=["telemetry"])
def streams(satellite_id: Optional[str] = None):
    """序列号水位与缺口（重启后不丢失）。"""
    return {"items": engine.stream_state(satellite_id)}


# ------------------------------------------------------------------ 告警
@app.get("/api/v1/alerts", tags=["alerts"])
def list_alerts(
    satellite_id: Optional[str] = None,
    point_id: Optional[str] = None,
    severity: list[str] = Query(default=None),
    status: list[str] = Query(default=None),
    rule_version: Optional[str] = None,
    config_switch: Optional[bool] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 100,
    offset: int = 0,
):
    """历史检索：按星/测点/级别/状态/规则版本/切换标记/时间过滤。"""
    if start_time and parse_iso(start_time) is None:
        raise HTTPException(400, "start_time 不是合法 ISO8601")
    if end_time and parse_iso(end_time) is None:
        raise HTTPException(400, "end_time 不是合法 ISO8601")
    return engine.list_alerts(
        satellite_id=satellite_id, point_id=point_id, severity=severity, status=status,
        start_ms=parse_iso(start_time) if start_time else None,
        end_ms=parse_iso(end_time) if end_time else None,
        rule_version=rule_version, config_switch=config_switch,
        limit=limit, offset=offset,
    )


@app.get("/api/v1/alerts/{alert_id}", tags=["alerts"])
def get_alert(alert_id: int):
    try:
        return engine.get_alert(alert_id)
    except KeyError:
        raise HTTPException(404, f"告警不存在: {alert_id}")


@app.post("/api/v1/alerts/{alert_id}/ack", tags=["alerts"])
def ack_alert(alert_id: int, body: AckIn):
    """告警确认。确认关系持久化，重启不丢。"""
    try:
        return engine.ack_alert(alert_id, body.operator, body.note)
    except KeyError:
        raise HTTPException(404, f"告警不存在: {alert_id}")


# ------------------------------------------------------------------ 抑制
@app.post("/api/v1/suppressions", tags=["suppression"])
def create_suppression(body: SuppressionIn, x_operator: str = "operator"):
    """新增维护窗口抑制；立即作用于窗口内既有活动告警。"""
    try:
        return engine.add_suppression(
            body.satellite_id, body.point_id, body.severities,
            body.start_time, body.end_time, body.reason, operator=x_operator,
        )
    except ValueError as exc:
        raise HTTPException(400, str(exc))


@app.get("/api/v1/suppressions", tags=["suppression"])
def list_suppressions(active_only: bool = False):
    at = now_ms() if active_only else None
    return {"items": engine.list_suppressions(at)}


@app.delete("/api/v1/suppressions/{suppression_id}", tags=["suppression"])
def remove_suppression(suppression_id: int, x_operator: str = "operator"):
    engine.delete_suppression(suppression_id, operator=x_operator)
    return {"deleted": suppression_id}


# ------------------------------------------------------------------ 历史
@app.get("/api/v1/history/samples", tags=["history"])
def history_samples(
    satellite_id: str,
    point_id: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    limit: int = 200,
):
    return engine.list_samples(
        satellite_id, point_id,
        parse_iso(start_time) if start_time else None,
        parse_iso(end_time) if end_time else None,
        limit=limit,
    )


@app.get("/api/v1/history/batches/{batch_id}", tags=["history"])
def history_batch(batch_id: str):
    """批次原始摘要（sha256、规则版本、计数、逐样本结论）。"""
    batch = engine.get_batch(batch_id)
    if batch is None:
        raise HTTPException(404, f"批次不存在: {batch_id}")
    return batch


# ------------------------------------------------------------------ 规则
@app.get("/api/v1/rules", tags=["rules"])
def get_rules():
    return {
        "active": engine.get_active_rules(),
        "versions": engine.list_rule_versions(),
        "watcher": {
            "file": str(watcher.path),
            "exists": watcher.path.exists(),
            "poll_interval_sec": watcher.interval,
            "last_error": watcher.last_error,
        },
    }


@app.post("/api/v1/rules", tags=["rules"])
def put_rules(body: RuleSetIn, x_operator: str = "operator"):
    """上传新版本规则集；activate=true 立即热生效，切换前后告警分别标注。"""
    try:
        return engine.put_ruleset(body.version, body.rules, body.activate,
                                  actor=x_operator)
    except ValueError as exc:
        raise HTTPException(400, str(exc))


@app.post("/api/v1/rules/{version}/activate", tags=["rules"])
def activate_rules(version: str, x_operator: str = "operator"):
    try:
        return engine.activate_ruleset(version, actor=x_operator)
    except KeyError as exc:
        raise HTTPException(404, str(exc))


@app.post("/api/v1/rules/reload", tags=["rules"])
def reload_rules_file():
    """强制从规则文件重新加载。"""
    result = watcher.load_once(force=True)
    if result is None:
        raise HTTPException(409, f"规则文件不可用或内容非法: {watcher.last_error}")
    return result


@app.get("/health", tags=["meta"])
def health():
    return {"status": "ok", "time": to_iso(now_ms()),
            "active_rule_version": engine.active_rule_version}
