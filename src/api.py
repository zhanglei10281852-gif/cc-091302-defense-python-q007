"""HTTP API 层（FastAPI）。

接口一览：
  POST /ingest/batch            批量接收遥测样本
  GET  /ingest/batches          接收批次留痕
  GET  /alerts                  告警历史检索（按测点/等级/状态/配置版本/时间过滤）
  GET  /alerts/{alert_id}       告警详情（含触发样本，可定位原始数据）
  POST /alerts/{alert_id}/ack   告警确认
  POST /suppressions            新建抑制规则    GET /suppressions  列表
  DELETE /suppressions/{id}     撤销抑制
  GET  /events                  数据质量与生命周期事件（缺口/重复/迟到/配置切换等）
  GET  /review-queue            待审队列        POST /review-queue/{id}/resolve 处置
  GET  /rules                   当前规则        PUT /rules  热更新（YAML/JSON）
  GET  /rules/history           规则版本历史
  GET  /points                  测点水位/窗口概览
  GET  /points/{point_id}/window 当前滑动窗口快照
  GET  /health                  健康检查
"""
from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from typing import Any, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from pydantic import BaseModel

from .models import IngestBatchIn, iso, to_epoch
from .service import Service, ServiceConfig


class AckIn(BaseModel):
    operator: str
    note: str = ""


class SuppressionIn(BaseModel):
    point_pattern: str
    level: Optional[str] = None
    starts_at: Any
    ends_at: Any
    reason: str = ""
    created_by: str = ""


class ReviewResolveIn(BaseModel):
    action: str            # ingest | drop
    operator: str
    corrected_ts: Any = None


def _alert_out(a: dict[str, Any], include_samples: bool = False) -> dict[str, Any]:
    out = {
        "alert_id": a["alert_id"],
        "point_id": a["point_id"],
        "level": a["level"],
        "state": a["state"],
        "rule_version": a["rule_version"],
        "config_version": a["config_version"],
        "opened_at": iso(a["opened_at"]),
        "updated_at": iso(a["updated_at"]),
        "resolved_at": iso(a.get("resolved_at")),
        "ack": ({"by": a["ack_by"], "at": iso(a["ack_at"]), "note": a["ack_note"]}
                if a.get("ack_by") else None),
        "suppressed_by": a.get("suppressed_by"),
        "violation_count": a["violation_count"],
        "message": a["message"],
        "stats": a.get("stats", {}),
        "locator": a.get("locator", {}),
    }
    if include_samples:
        out["trigger_samples"] = [
            {"point_id": s["point_id"], "seq": s["seq"], "ts": iso(s["ts"]),
             "value": s["value"], "config_version": s["config_version"]}
            for s in a.get("trigger_samples", [])
        ]
    return out


def _event_out(e: dict[str, Any]) -> dict[str, Any]:
    return {"id": e["id"], "kind": e["kind"], "point_id": e["point_id"],
            "payload": e["payload"], "created_at": iso(e["created_at"])}


def _review_out(r: dict[str, Any]) -> dict[str, Any]:
    return {"id": r["id"], "sample": r["sample"], "reason": r["reason"],
            "status": r["status"], "created_at": iso(r["created_at"]),
            "resolved_at": iso(r.get("resolved_at")), "resolution": r.get("resolution")}


def _supp_out(s: dict[str, Any], now: float) -> dict[str, Any]:
    active = (not s["revoked"]) and s["starts_at"] <= now <= s["ends_at"]
    return {"supp_id": s["supp_id"], "point_pattern": s["point_pattern"],
            "level": s["level"], "starts_at": iso(s["starts_at"]),
            "ends_at": iso(s["ends_at"]), "reason": s["reason"],
            "created_by": s["created_by"], "created_at": iso(s["created_at"]),
            "revoked": bool(s["revoked"]), "active": active}


def create_app(service: Optional[Service] = None,
               tick_interval_sec: float = 1.0) -> FastAPI:
    svc = service or Service()
    core = svc.core

    app = FastAPI(title="卫星遥测异常告警服务", version="1.0.0")

    async def _ticker() -> None:
        while True:
            await asyncio.sleep(tick_interval_sec)
            try:
                core.tick()
            except Exception:
                app.state.last_tick_error = time.time()

    @asynccontextmanager
    async def _lifespan(_app: FastAPI):
        task = asyncio.create_task(_ticker())
        try:
            yield
        finally:
            task.cancel()

    app.router.lifespan_context = _lifespan

    # ---------------- 接收 ----------------

    @app.post("/ingest/batch")
    def ingest_batch(batch: IngestBatchIn) -> Any:
        return core.ingest_batch(batch).model_dump()

    @app.get("/ingest/batches")
    def list_batches(limit: int = Query(50, ge=1, le=500),
                     offset: int = Query(0, ge=0)) -> Any:
        rows = svc.storage.batch_list(limit, offset)
        return [{"batch_id": b["batch_id"], "source": b["source"],
                 "received_at": iso(b["received_at"]), "summary": b["summary"]}
                for b in rows]

    # ---------------- 告警 ----------------

    @app.get("/alerts")
    def list_alerts(point_id: Optional[str] = None, level: Optional[str] = None,
                    state: Optional[str] = None, config_version: Optional[str] = None,
                    since: Optional[str] = None, until: Optional[str] = None,
                    limit: int = Query(100, ge=1, le=1000),
                    offset: int = Query(0, ge=0)) -> Any:
        rows = svc.storage.query_alerts(
            point_id=point_id, level=level, state=state,
            config_version=config_version,
            since=to_epoch(since), until=to_epoch(until),
            limit=limit, offset=offset)
        return [_alert_out(a) for a in rows]

    @app.get("/alerts/{alert_id}")
    def get_alert(alert_id: str) -> Any:
        a = core.get_alert_detail(alert_id)
        if not a:
            raise HTTPException(404, "告警不存在")
        return _alert_out(a, include_samples=True)

    @app.post("/alerts/{alert_id}/ack")
    def ack_alert(alert_id: str, body: AckIn) -> Any:
        a = core.ack_alert(alert_id, body.operator, body.note)
        if not a:
            raise HTTPException(404, "告警不存在")
        return _alert_out(a)

    # ---------------- 抑制 ----------------

    @app.post("/suppressions", status_code=201)
    def add_suppression(body: SuppressionIn) -> Any:
        start, end = to_epoch(body.starts_at), to_epoch(body.ends_at)
        if start is None or end is None or end <= start:
            raise HTTPException(422, "starts_at/ends_at 必须是有效时间且 ends_at 更晚")
        supp = core.add_suppression(body.point_pattern, body.level, start, end,
                                    body.reason, body.created_by)
        return _supp_out({**supp, "revoked": 0}, core.clock())

    @app.get("/suppressions")
    def list_suppressions() -> Any:
        now = core.clock()
        return [_supp_out(s, now) for s in svc.storage.suppression_list()]

    @app.delete("/suppressions/{supp_id}")
    def revoke_suppression(supp_id: str) -> Any:
        if not core.revoke_suppression(supp_id):
            raise HTTPException(404, "抑制规则不存在")
        return {"revoked": supp_id}

    # ---------------- 事件 / 待审 ----------------

    @app.get("/events")
    def list_events(kind: Optional[str] = None, point_id: Optional[str] = None,
                    since: Optional[str] = None, until: Optional[str] = None,
                    limit: int = Query(200, ge=1, le=1000),
                    offset: int = Query(0, ge=0)) -> Any:
        rows = svc.storage.query_events(kind=kind, point_id=point_id,
                                        since=to_epoch(since), until=to_epoch(until),
                                        limit=limit, offset=offset)
        return [_event_out(e) for e in rows]

    @app.get("/review-queue")
    def list_review(status: Optional[str] = None,
                    limit: int = Query(200, ge=1, le=1000),
                    offset: int = Query(0, ge=0)) -> Any:
        return [_review_out(r) for r in svc.storage.review_list(status, limit, offset)]

    @app.post("/review-queue/{rid}/resolve")
    def resolve_review(rid: int, body: ReviewResolveIn) -> Any:
        try:
            entry = core.review_resolve(rid, body.action, body.operator,
                                        body.corrected_ts)
        except ValueError as exc:
            raise HTTPException(422, str(exc))
        if not entry:
            raise HTTPException(404, "待审项不存在或已处置")
        return _review_out(entry)

    # ---------------- 规则 ----------------

    @app.get("/rules")
    def get_rules() -> Any:
        return {"version": core.rules.version,
                "rules": core.rules.model_dump()}

    @app.put("/rules")
    async def put_rules(request: Request) -> Any:
        content = (await request.body()).decode("utf-8")
        try:
            rs = core.reload_rules(content, applied_by="api")
        except Exception as exc:
            raise HTTPException(422, f"规则无效: {exc}")
        return {"version": rs.version, "applied": True}

    @app.get("/rules/history")
    def rules_history() -> Any:
        return [{"id": r["id"], "version": r["version"],
                 "applied_at": iso(r["applied_at"]), "applied_by": r["applied_by"]}
                for r in svc.storage.rules_history()]

    # ---------------- 测点 ----------------

    @app.get("/points")
    def points() -> Any:
        return core.points_overview()

    @app.get("/points/{point_id}/window")
    def point_window(point_id: str) -> Any:
        snap = core.window_snapshot(point_id)
        if snap is None:
            raise HTTPException(404, "测点不存在")
        return snap

    @app.get("/health")
    def health() -> Any:
        return {"ready": svc.ready, "rule_version": core.rules.version}

    return app


app = create_app()

if __name__ == "__main__":
    import uvicorn

    uvicorn.run("src.api:app", host="0.0.0.0", port=8000, reload=False)
