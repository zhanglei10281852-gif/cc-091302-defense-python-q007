"""告警链路：分级、聚合防风暴、确认、自动恢复与冷却、配置版本标注、抑制、热更新。"""
from src.models import IngestBatchIn
from tests.conftest import make_batch, sample

T0 = 1_000_000.0


def ingest(service, samples):
    return service.core.ingest_batch(IngestBatchIn.model_validate(make_batch(samples)))


def alerts(service, **kw):
    return service.storage.query_alerts(**kw)


def test_graded_alerts_warning_then_critical(service):
    ingest(service, [sample(seq=i, value=6.0, ts=T0 + i) for i in (1, 2, 3)])
    rows = alerts(service)
    assert len(rows) == 1
    assert rows[0]["level"] == "warning"
    assert rows[0]["violation_count"] == 3

    ingest(service, [sample(seq=4, value=11.0, ts=T0 + 3)])
    rows = alerts(service)
    levels = sorted(a["level"] for a in rows)
    assert levels == ["critical", "warning"]
    warning = next(a for a in rows if a["level"] == "warning")
    assert warning["violation_count"] == 4  # 11 同样越 warning 限，聚合计入


def test_aggregation_prevents_alert_storm(service):
    ingest(service, [sample(seq=i, value=6.0, ts=T0 + i) for i in (1, 2, 3)])
    ingest(service, [sample(seq=i, value=7.0, ts=T0 + i) for i in (4, 5, 6)])
    rows = alerts(service)
    assert len(rows) == 1  # 持续违规聚合为同一未确认告警，不产生风暴
    assert rows[0]["violation_count"] == 6
    detail = service.core.get_alert_detail(rows[0]["alert_id"])
    assert sorted(s["seq"] for s in detail["trigger_samples"]) == [1, 2, 3, 4, 5, 6]


def test_trigger_samples_locate_raw_data(service):
    ingest(service, [sample(seq=i, value=6.0, ts=T0 + i) for i in (1, 2, 3)])
    alert = alerts(service)[0]
    detail = service.core.get_alert_detail(alert["alert_id"])
    # 值班员可凭 locator + trigger_samples 定位原始样本
    assert detail["locator"]["seq_min"] == 1
    assert detail["locator"]["seq_max"] == 3
    assert detail["locator"]["point_id"] == "ATT.ROLL"
    assert {s["seq"] for s in detail["trigger_samples"]} == {1, 2, 3}
    assert all(s["config_version"] == "cfg-A" for s in detail["trigger_samples"])
    assert detail["rule_version"] == "test-rules-1"


def test_ack_transitions_and_cooldown(service, clock):
    ingest(service, [sample(seq=i, value=6.0, ts=T0 + i) for i in (1, 2, 3)])
    alert = alerts(service)[0]
    acked = service.core.ack_alert(alert["alert_id"], operator="zhang", note="已知悉")
    assert acked["state"] == "acknowledged"
    assert acked["ack_by"] == "zhang"

    # 确认后冷却期内复发不重开告警
    clock.advance(10)
    ingest(service, [sample(seq=i, value=6.0, ts=clock() + i) for i in (4, 5, 6)])
    assert len(alerts(service)) == 1
    assert service.storage.query_events(kind="alert_cooldown")

    # 冷却期过后复发重新开警
    clock.advance(31)
    ingest(service, [sample(seq=i, value=6.0, ts=clock() + i) for i in (7, 8, 9)])
    rows = alerts(service)
    assert len(rows) == 2
    assert sorted(a["state"] for a in rows) == ["acknowledged", "active"]


def test_auto_resolve_after_violations_leave_window(service, clock):
    ingest(service, [sample(seq=i, value=6.0, ts=T0 + i) for i in (1, 2, 3)])
    alert = alerts(service)[0]
    assert alert["state"] == "active"
    clock.advance(100)  # 数据停流，墙上时钟推进使违规滑出窗口
    service.core.tick()
    resolved = service.storage.get_alert(alert["alert_id"])
    assert resolved["state"] == "resolved"
    assert resolved["resolved_at"] is not None


def test_config_switch_labeled_separately(service, clock):
    # 配置版本 cfg-A 下的越限
    ingest(service, [sample(seq=i, value=6.0, ts=T0 + i, config_version="cfg-A")
                     for i in (1, 2, 3)])
    # 切换到 cfg-B（正常样本 + 越限样本）
    clock.advance(1)
    ingest(service, [sample(seq=i, value=6.0, ts=T0 + 3 + i, config_version="cfg-B")
                     for i in (4, 5, 6)])
    rows = alerts(service)
    assert len(rows) == 2
    by_cfg = {a["config_version"]: a for a in rows}
    assert set(by_cfg) == {"cfg-A", "cfg-B"}  # 切换前后分别标注
    assert all(a["level"] == "warning" for a in rows)
    events = service.storage.query_events(kind="config_switch")
    assert events and events[0]["payload"] == {"from": "cfg-A", "to": "cfg-B",
                                               "ts": T0 + 7}


def test_config_segment_min_samples_isolated(service):
    # 切换后旧版本样本仍在窗口内，但新版本段样本不足 → 不为新版本开警
    ingest(service, [sample(seq=i, value=6.0, ts=T0 + i, config_version="cfg-A")
                     for i in (1, 2, 3)])
    ingest(service, [sample(seq=4, value=9.0, ts=T0 + 4, config_version="cfg-B")])
    rows = alerts(service)
    assert len(rows) == 1
    assert rows[0]["config_version"] == "cfg-A"


def test_suppression_silences_matching_alerts(service, clock):
    service.core.add_suppression("ATT.*", "warning", clock() - 1, clock() + 600,
                                 reason="轨道切换维护窗口", created_by="ops")
    ingest(service, [sample(seq=i, value=6.0, ts=T0 + i) for i in (1, 2, 3)])
    rows = alerts(service)
    assert len(rows) == 1
    assert rows[0]["state"] == "suppressed"
    assert rows[0]["suppressed_by"]
    assert alerts(service, state="active") == []


def test_suppression_sweeps_existing_active_alerts(service, clock):
    ingest(service, [sample(seq=i, value=6.0, ts=T0 + i) for i in (1, 2, 3)])
    supp = service.core.add_suppression("ATT.ROLL", None, clock() - 1, clock() + 600,
                                        reason="计划内机动", created_by="ops")
    rows = alerts(service)
    assert rows[0]["state"] == "suppressed"
    assert rows[0]["suppressed_by"] == supp["supp_id"]


def test_jitter_does_not_create_value_alerts(service):
    # 通信抖动：乱序 + 重复 + 缺口，但数值正常 → 只有数据质量事件，无告警
    ingest(service, [
        sample(seq=1, value=0.5, ts=T0), sample(seq=3, value=0.6, ts=T0 + 2),
        sample(seq=2, value=0.4, ts=T0 + 1), sample(seq=2, value=0.4, ts=T0 + 1),
        sample(seq=8, value=0.5, ts=T0 + 7),
    ])
    assert alerts(service) == []
    kinds = {e["kind"] for e in service.storage.query_events()}
    assert {"seq_gap", "duplicate"} <= kinds


def test_hot_rule_reload_versions_alerts(service, clock):
    ingest(service, [sample(seq=i, value=6.0, ts=T0 + i) for i in (1, 2, 3)])
    old = alerts(service)[0]
    assert old["rule_version"] == "test-rules-1"

    # 热更新：阈值收紧到 abs>1，版本号变更
    new_rules = service.storage.rules_latest()["content"].replace(
        "abs_gt: 5.0", "abs_gt: 1.0").replace("test-rules-1", "test-rules-2")
    service.core.reload_rules(new_rules, applied_by="ops")
    service.core.ack_alert(old["alert_id"], operator="ops")

    clock.advance(100)
    ingest(service, [sample(seq=i, value=2.0, ts=clock() + i) for i in (4, 5, 6)])
    rows = alerts(service, state="active")
    assert len(rows) == 1
    assert rows[0]["rule_version"] == "test-rules-2"  # 新告警用新规则版本
    assert service.storage.get_alert(old["alert_id"])["rule_version"] == "test-rules-1"
    versions = [r["version"] for r in service.storage.rules_history()]
    assert versions[:2] == ["test-rules-2", "test-rules-1"]


def test_history_filters(service):
    ingest(service, [sample(seq=i, value=6.0, ts=T0 + i) for i in (1, 2, 3)])
    ingest(service, [sample(seq=4, value=11.0, ts=T0 + 4)])
    assert len(alerts(service, level="critical")) == 1
    assert len(alerts(service, level="warning")) == 1
    assert len(alerts(service, config_version="cfg-A")) == 2
    assert alerts(service, config_version="cfg-Z") == []
    assert len(alerts(service, since=T0 - 10, until=T0 + 10)) == 2
    assert alerts(service, since=T0 + 100) == []
