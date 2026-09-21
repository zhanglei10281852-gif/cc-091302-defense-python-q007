"""端到端测试：乱序/丢包、去重、滑动窗口分级、热更新、抑制、待审、持久化、检索。"""
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone

from src.engine import Engine
from src.models import BatchIn, SampleIn
from src.storage import Storage

BASE = "2026-09-21T00:00:00+00:00"  # 固定过去时间，避免未来钟偏；窗口按相对评估


def iso(sec_from_base: float) -> str:
    base = datetime.fromisoformat(BASE)
    return (base + timedelta(seconds=sec_from_base)).strftime("%Y-%m-%dT%H:%M:%S.") + \
        f"{int((sec_from_base % 1) * 1000):03d}Z"


def now_iso(offset_s: float = 0) -> str:
    return (datetime.now(timezone.utc) + timedelta(seconds=offset_s)).isoformat()


def sample(seq, point, value, t, config_version="cfg-1", quality="good"):
    return {"seq": seq, "point_id": point, "value": value, "sample_time": t,
            "config_version": config_version, "time_quality": quality}


RULES_V1 = {
    "p.wc": {"min": 0, "max": 10, "window_sec": 60,
             "warning_count": 2, "critical_count": 4},
    "p.spike": {"min": 0, "max": 10, "window_sec": 60, "warning_count": 2},
    "p.pending": {"min": 0, "max": 10, "window_sec": 60, "warning_count": 1},
    "p.supp": {"min": 0, "max": 10, "window_sec": 120, "warning_count": 1},
    "p.switch": {"min": 0, "max": 40, "window_sec": 60, "warning_count": 1},
    "p.file": {"min": 0, "max": 5, "window_sec": 60, "warning_count": 1},
}

RULES_V2 = {
    "p.wc": {"min": 0, "max": 10, "window_sec": 60,
             "warning_count": 2, "critical_count": 4},
    "p.spike": {"min": 0, "max": 10, "window_sec": 60, "warning_count": 2},
    "p.pending": {"min": 0, "max": 10, "window_sec": 60, "warning_count": 1},
    "p.supp": {"min": 0, "max": 10, "window_sec": 120, "warning_count": 1},
    "p.switch": {"min": 0, "max": 5, "window_sec": 60, "warning_count": 1},
    "p.file": {"min": 0, "max": 5, "window_sec": 60, "warning_count": 1},
}


def test_00_install_rules(client):
    r = client.post("/api/v1/rules",
                    json={"version": "test-v1", "rules": RULES_V1, "activate": True})
    assert r.status_code == 200, r.text
    assert r.json()["switched"] in (True, False)


# ---------------------------------------------------------- 抖动 vs 故障
def test_single_spike_is_communication_jitter(client):
    sat = "SAT-SPIKE"
    body = {"satellite_id": sat, "samples": [
        sample(1, "p.spike", 5, iso(1)),
        sample(2, "p.spike", 99, iso(2)),   # 单点尖峰
        sample(3, "p.spike", 5, iso(3)),
    ]}
    r = client.post("/api/v1/telemetry/batch", json=body)
    assert r.status_code == 200
    data = r.json()
    assert data["alerts"]["created"] == []
    ev = [e for e in data["evaluated"] if e["point_id"] == "p.spike"][0]
    assert ev["violations_in_window"] == 1 and ev["severity"] is None


def test_warning_then_critical_escalation(client):
    sat = "SAT-WC"
    r = client.post("/api/v1/telemetry/batch", json={"satellite_id": sat, "samples": [
        sample(1, "p.wc", 50, iso(100)), sample(2, "p.wc", 60, iso(101)),
    ]})
    data = r.json()
    assert len(data["alerts"]["created"]) == 1
    alert = data["alerts"]["created"][0]
    assert alert["severity"] == "warning" and alert["status"] == "active"
    assert alert["rule_version"] == "test-v1"
    # 值班员可直接定位触发样本
    refs = alert["trigger_sample_refs"]
    assert {x["seq"] for x in refs} == {1, 2}
    assert refs[0]["locator"] == f"{sat}/p.wc#1"

    r = client.post("/api/v1/telemetry/batch", json={"satellite_id": sat, "samples": [
        sample(3, "p.wc", 70, iso(102)), sample(4, "p.wc", 80, iso(103)),
    ]})
    updated = r.json()["alerts"]["updated"]
    assert len(updated) == 1 and updated[0]["severity"] == "critical"

    # 恢复后自动关闭
    r = client.post("/api/v1/telemetry/batch", json={"satellite_id": sat, "samples": [
        sample(5, "p.wc", 5, iso(200)),  # 窗口滑出所有违规样本
    ]})
    assert r.json()["alerts"]["recovered"][0]["id"] == alert["id"]


# ---------------------------------------------------------- 乱序与丢包
def test_out_of_order_gap_and_late_fill(client):
    sat = "SAT-GAP"
    r = client.post("/api/v1/telemetry/batch", json={"satellite_id": sat, "samples": [
        sample(1, "p.spike", 1, iso(1)),
        sample(2, "p.spike", 2, iso(2)),
        sample(4, "p.spike", 4, iso(4)),
        sample(5, "p.spike", 5, iso(5)),
    ]})
    data = r.json()
    assert data["high_water"] == 5
    assert data["missing_sequences"] == [3]

    # 迟到乱序样本回填缺口
    r = client.post("/api/v1/telemetry/batch", json={"satellite_id": sat, "samples": [
        sample(3, "p.spike", 3, iso(3)),
    ]})
    data = r.json()
    assert data["out_of_order"] == 1 and data["gap_filled"] == 1
    assert data["missing_sequence_count"] == 0

    state = client.get(f"/api/v1/streams?satellite_id={sat}").json()["items"][0]
    assert state["high_water"] == 5 and state["missing_sequences"] == []


# ---------------------------------------------------------- 重复去重
def test_duplicate_samples_not_double_counted(client):
    sat = "SAT-DUP"
    payload = {"satellite_id": sat, "samples": [
        sample(1, "p.wc", 50, iso(300)), sample(2, "p.wc", 60, iso(301)),
    ]}
    r1 = client.post("/api/v1/telemetry/batch", json=payload).json()
    assert len(r1["alerts"]["created"]) == 1

    # 维护窗口内重发：同样的序列号
    r2 = client.post("/api/v1/telemetry/batch", json={
        "satellite_id": sat,
        "samples": [sample(1, "p.wc", 50, iso(300)), sample(2, "p.wc", 60, iso(301))],
    }).json()
    assert r2["duplicated"] == 2 and r2["accepted"] == 0
    assert r2["alerts"] == {"created": [], "updated": [], "recovered": []}

    # batch_id 相同则整体幂等回放
    payload["batch_id"] = "dup-batch-1"
    a = client.post("/api/v1/telemetry/batch", json=payload).json()
    b = client.post("/api/v1/telemetry/batch", json=payload).json()
    assert b["replayed"] is True and b["batch_id"] == a["batch_id"]


# ---------------------------------------------------------- 待审队列
def test_pending_queue_and_resolve(client):
    sat = "SAT-PEND"
    r = client.post("/api/v1/telemetry/batch", json={"satellite_id": sat, "samples": [
        {"seq": 1, "point_id": "p.pending", "value": 99, "sample_time": None},
        sample(2, "p.pending", 99, now_iso(), quality="bad"),
        sample(3, "p.pending", 99, now_iso(3600)),  # 超出钟偏的未来时间
    ]})
    data = r.json()
    assert data["pending"] == 3 and data["accepted"] == 0
    reasons = {p["reason"] for p in data["pending_items"]}
    assert reasons == {"missing_or_bad_time", "time_quality_bad", "future_time_beyond_skew"}

    pending = client.get(f"/api/v1/telemetry/pending?satellite_id={sat}").json()["items"]
    pid = next(p["id"] for p in pending if p["reason"] == "missing_or_bad_time")

    # 修正时间后接收：立即参与评估并产生告警
    r = client.post(f"/api/v1/telemetry/pending/{pid}/resolve", json={
        "action": "accept", "corrected_time": iso(400), "note": "地面校时",
    })
    assert r.status_code == 200
    ingest = r.json()["ingest"]
    assert ingest["accepted"] == 1
    assert len(ingest["alerts"]["created"]) == 1

    # 拒绝不产生样本
    pid2 = pending[0]["id"] if pending[0]["id"] != pid else pending[1]["id"]
    r = client.post(f"/api/v1/telemetry/pending/{pid2}/resolve",
                    json={"action": "reject", "note": "脏数据"})
    assert r.json()["status"] == "rejected"


def status_of(client, alert_id):
    return client.get(f"/api/v1/alerts/{alert_id}").json()["status"]


# ---------------------------------------------------------- 确认 + 抑制
def test_ack_suppression_and_reapply(client):
    sat = "SAT-SUPP"
    alert_id = client.post("/api/v1/telemetry/batch", json={"satellite_id": sat, "samples": [
        sample(1, "p.supp", 50, now_iso(-1)),
    ]}).json()["alerts"]["created"][0]["id"]

    r = client.post(f"/api/v1/alerts/{alert_id}/ack",
                    json={"operator": "zhang", "note": "轨道切换预期"})
    body = r.json()
    assert body["changed"] is True and body["status"] == "acked" and body["acked_by"] == "zhang"
    # 重复确认幂等
    assert client.post(f"/api/v1/alerts/{alert_id}/ack",
                       json={"operator": "li"}).json()["changed"] is False

    # 维护窗口事后下发也要立即抑制既有告警
    r = client.post("/api/v1/suppressions", json={
        "satellite_id": sat, "point_id": "p.supp",
        "severities": ["warning", "critical"],
        "start_time": now_iso(-60), "end_time": now_iso(60),
        "reason": "轨道维持",
    })
    supp = r.json()
    assert alert_id in supp["applied_alert_ids"]
    assert status_of(client, alert_id) == "suppressed"

    # 维护窗口取消后，新违规重新可见
    client.delete(f"/api/v1/suppressions/{supp['id']}")
    client.post("/api/v1/telemetry/batch", json={"satellite_id": sat, "samples": [
        sample(2, "p.supp", 55, now_iso(0)),
    ]})
    assert status_of(client, alert_id) == "acked"  # 已确认状态保持，不再被抑制

    active_supp = client.get("/api/v1/suppressions?active_only=true").json()["items"]
    assert all(s["satellite_id"] != sat for s in active_supp)


def test_suppression_does_not_swallow_new_alerts(client):
    sat = "SAT-SUPP2"
    client.post("/api/v1/suppressions", json={
        "satellite_id": sat, "point_id": "p.supp", "severities": ["warning"],
        "start_time": now_iso(-60), "end_time": now_iso(60), "reason": "窗口",
    })
    data = client.post("/api/v1/telemetry/batch", json={"satellite_id": sat, "samples": [
        sample(1, "p.supp", 50, now_iso(0)),
    ]}).json()
    alert = data["alerts"]["created"][0]
    # 抑制不等于消失：留痕、可检索、带 suppression_id
    assert alert["status"] == "suppressed" and alert["suppression_id"] is not None
    found = client.get("/api/v1/alerts", params={
        "satellite_id": sat, "status": ["suppressed"]}).json()
    assert found["total"] == 1


# ---------------------------------------------------------- 配置切换标注
def test_rule_hot_switch_marks_before_after(client):
    sat = "SAT-SWITCH"
    old = client.post("/api/v1/telemetry/batch", json={"satellite_id": sat, "samples": [
        sample(1, "p.switch", 50, now_iso(-2), config_version="cfg-A"),
    ]}).json()["alerts"]["created"][0]
    assert old["config_version"] == "cfg-A"

    r = client.post("/api/v1/rules",
                    json={"version": "test-v2", "rules": RULES_V2, "activate": True})
    assert r.json()["switched"] is True

    closed = client.get(f"/api/v1/alerts/{old['id']}").json()
    assert closed["status"] == "closed" and closed["close_reason"] == "rule_switch"
    assert closed["config_switch"] is True
    assert closed["config_phase"] == "before_switch"

    # v2 下 50 现在越界（上限改为 5），产生独立的新告警
    new = client.post("/api/v1/telemetry/batch", json={"satellite_id": sat, "samples": [
        sample(2, "p.switch", 50, now_iso(-1), config_version="cfg-B"),
    ]}).json()["alerts"]["created"][0]
    assert new["rule_version"] == "test-v2"
    assert new["config_version"] == "cfg-B" and new["config_switch"] is False

    before = client.get("/api/v1/alerts", params={
        "satellite_id": sat, "config_switch": True}).json()
    after = client.get("/api/v1/alerts", params={
        "satellite_id": sat, "rule_version": "test-v2"}).json()
    assert before["total"] == 1 and after["items"][0]["id"] == new["id"]

    # 切回 v1 供后续测试
    client.post("/api/v1/rules/test-v1/activate")


def test_rule_file_hot_reload(client, rules_file):
    rules_file.write_text(json.dumps({
        "version": "test-file-v1",
        "rules": {"p.file": {"min": 0, "max": 5, "window_sec": 60, "warning_count": 1}},
    }, ensure_ascii=False), encoding="utf-8")
    r = client.post("/api/v1/rules/reload")
    assert r.status_code == 200 and r.json()["version"] == "test-file-v1"
    info = client.get("/api/v1/rules").json()
    assert info["active"]["version"] == "test-file-v1"

    # 坏文件不能冲掉现行规则
    rules_file.write_text("{ not json", encoding="utf-8")
    r = client.post("/api/v1/rules/reload")
    assert r.status_code == 409
    assert client.get("/health").json()["active_rule_version"] == "test-file-v1"
    client.post("/api/v1/rules/test-v1/activate")


# ---------------------------------------------------------- 历史与留痕
def test_history_and_raw_batch_summary(client):
    sat = "SAT-HIST"
    data = client.post("/api/v1/telemetry/batch", json={
        "satellite_id": sat, "batch_id": "hist-b1", "samples": [
            sample(1, "p.wc", 50, iso(500)), sample(2, "p.wc", 51, iso(501)),
        ]}).json()

    batch = client.get("/api/v1/history/batches/hist-b1").json()
    assert batch["payload_sha256"] and len(batch["payload_sha256"]) == 64
    assert batch["rule_version"] == "test-v1"
    assert batch["sample_count"] == 2
    assert {s["seq"]: s["status"] for s in batch["result"]["samples"]} == {1: "accepted", 2: "accepted"}

    samples = client.get("/api/v1/history/samples", params={
        "satellite_id": sat, "point_id": "p.wc"}).json()["items"]
    assert len(samples) == 2 and samples[0]["batch_id"] == "hist-b1"

    alerts = client.get("/api/v1/alerts", params={
        "satellite_id": sat, "severity": ["warning"], "status": ["active"]}).json()
    assert alerts["total"] == 1
    detail = client.get(f"/api/v1/alerts/{alerts['items'][0]['id']}").json()
    locs = [r["locator"] for r in detail["trigger_sample_refs"]]
    assert f"{sat}/p.wc#1" in locs


# ---------------------------------------------------------- 重启持久化
def test_restart_preserves_alerts_and_watermark(client, db_path):
    sat = "SAT-PERSIST"
    resp = client.post("/api/v1/telemetry/batch", json={"satellite_id": sat, "samples": [
        sample(1, "p.wc", 9, iso(600)),
        sample(2, "p.wc", 99, iso(601)),
        sample(4, "p.wc", 99, iso(603)),
    ]}).json()
    alert_id = resp["alerts"]["created"][0]["id"]
    client.post(f"/api/v1/alerts/{alert_id}/ack",
                json={"operator": "night-op", "note": "重启前确认"})

    # 用全新的 Storage/Engine 打开同一数据库，模拟进程重启
    storage2 = Storage(db_path)
    try:
        engine2 = Engine(storage2)
        assert engine2.active_rule_version == "test-v1"
        alert = engine2.get_alert(alert_id)
        assert alert["status"] == "acked" and alert["acked_by"] == "night-op"
        state = engine2.stream_state(sat)[0]
        assert state["high_water"] == 4 and state["missing_sequences"] == [3]

        # 重启后补缺口仍能正确回填
        result = engine2.ingest_batch(BatchIn(satellite_id=sat, samples=[
            SampleIn(seq=3, point_id="p.wc", value=5, sample_time=iso(602))]))
        assert result["gap_filled"] == 1 and result["missing_sequence_count"] == 0
    finally:
        storage2.close()
