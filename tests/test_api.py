"""HTTP API 冒烟测试。"""
import pytest
from fastapi.testclient import TestClient

from src.api import create_app
from tests.conftest import TEST_RULES, sample

T0 = 1_000_000.0


@pytest.fixture()
def client(service):
    service.core.reload_rules(TEST_RULES, applied_by="test")
    with TestClient(create_app(service)) as c:
        yield c


def test_full_alert_lifecycle_over_http(client):
    r = client.post("/ingest/batch", json={
        "source": "gw-1",
        "samples": [sample(seq=i, value=6.0, ts=T0 + i) for i in (1, 2, 3)],
    })
    assert r.status_code == 200
    body = r.json()
    assert body["accepted"] == 3 and body["total"] == 3

    rows = client.get("/alerts", params={"level": "warning"}).json()
    assert len(rows) == 1
    alert_id = rows[0]["alert_id"]

    detail = client.get(f"/alerts/{alert_id}").json()
    assert detail["rule_version"] == "test-rules-1"
    assert detail["config_version"] == "cfg-A"
    assert {s["seq"] for s in detail["trigger_samples"]} == {1, 2, 3}
    assert detail["locator"]["seq_min"] == 1

    r = client.post(f"/alerts/{alert_id}/ack",
                    json={"operator": "li", "note": "现场确认"})
    assert r.status_code == 200
    assert r.json()["state"] == "acknowledged"
    assert r.json()["ack"]["by"] == "li"

    assert client.get("/alerts", params={"state": "active"}).json() == []
    assert len(client.get("/alerts", params={"state": "acknowledged"}).json()) == 1


def test_ingest_validation_and_review_flow(client):
    r = client.post("/ingest/batch", json={"samples": [
        sample(seq=1, ts=None),
        sample(seq=2, ts=T0, time_quality="unsynced"),
    ]})
    assert r.status_code == 200
    assert r.json()["review"] == 2

    pending = client.get("/review-queue", params={"status": "pending"}).json()
    assert len(pending) == 2
    rid = pending[0]["id"]
    r = client.post(f"/review-queue/{rid}/resolve",
                    json={"action": "ingest", "operator": "ops",
                          "corrected_ts": T0 + 5})
    assert r.status_code == 200
    assert r.json()["status"] == "ingested"

    # 非法样本（缺字段）→ 422
    r = client.post("/ingest/batch", json={"samples": [{"point_id": "X"}]})
    assert r.status_code == 422


def test_suppression_and_events_and_points(client):
    r = client.post("/suppressions", json={
        "point_pattern": "ATT.*", "level": "warning",
        "starts_at": T0 - 10, "ends_at": T0 + 1000,
        "reason": "轨道切换", "created_by": "ops"})
    assert r.status_code == 201
    supp_id = r.json()["supp_id"]
    assert r.json()["active"] is True

    client.post("/ingest/batch", json={
        "samples": [sample(seq=i, value=6.0, ts=T0 + i) for i in (1, 2, 3)]})
    rows = client.get("/alerts").json()
    assert rows[0]["state"] == "suppressed"

    events = client.get("/events", params={"kind": "alert_suppressed"}).json()
    assert len(events) == 1

    points = client.get("/points").json()
    assert points[0]["point_id"] == "ATT.ROLL"
    assert points[0]["high_seq"] == 3

    window = client.get("/points/ATT.ROLL/window").json()
    assert len(window["samples"]) == 3

    r = client.delete(f"/suppressions/{supp_id}")
    assert r.status_code == 200
    assert client.get("/suppressions").json()[0]["revoked"] is True


def test_rules_hot_reload_over_http(client):
    r = client.get("/rules")
    assert r.json()["version"] == "test-rules-1"

    new_rules = TEST_RULES.replace("test-rules-1", "test-rules-2")
    r = client.put("/rules", content=new_rules,
                   headers={"content-type": "application/yaml"})
    assert r.status_code == 200
    assert r.json()["version"] == "test-rules-2"
    assert client.get("/rules").json()["version"] == "test-rules-2"

    history = client.get("/rules/history").json()
    assert "test-rules-2" in [h["version"] for h in history]

    r = client.put("/rules", content="not: [valid",
                   headers={"content-type": "application/yaml"})
    assert r.status_code == 422


def test_batches_health_and_404(client):
    client.post("/ingest/batch", json={"samples": [sample(seq=1, ts=T0)]})
    batches = client.get("/ingest/batches").json()
    assert len(batches) == 1 and batches[0]["summary"]["accepted"] == 1

    health = client.get("/health").json()
    assert health["ready"] is True

    assert client.get("/alerts/al-nonexistent").status_code == 404
    assert client.post("/alerts/al-nonexistent/ack",
                       json={"operator": "x"}).status_code == 404
    assert client.get("/points/NOPE/window").status_code == 404
