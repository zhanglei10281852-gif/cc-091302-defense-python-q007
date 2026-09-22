"""重启持久化：未确认告警、序列号水位、去重状态、规则版本不丢失。"""
from src.models import IngestBatchIn
from src.service import Service, ServiceConfig
from src.engine import EngineConfig
from tests.conftest import TEST_RULES, make_batch, sample

T0 = 1_000_000.0


def build(db_path, clock):
    svc = Service(
        ServiceConfig(db_path=str(db_path), rules_path=None,
                      engine=EngineConfig(max_past_skew_sec=86400.0)),
        clock=clock,
    )
    return svc


def ingest(service, samples):
    return service.core.ingest_batch(IngestBatchIn.model_validate(make_batch(samples)))


def test_restart_preserves_unacked_alerts_watermark_and_dedup(tmp_path, clock):
    db = tmp_path / "telemetry.db"
    svc = build(db, clock)
    svc.core.reload_rules(TEST_RULES, applied_by="test")
    ingest(svc, [sample(seq=i, value=6.0, ts=T0 + i) for i in (1, 2, 3)])
    alert = svc.storage.query_alerts()[0]
    assert alert["state"] == "active"
    svc.close()

    # 模拟重启：同一数据库文件重新装配
    svc2 = build(db, clock)
    try:
        # 未确认告警仍在，且内存索引已恢复
        rows = svc2.storage.query_alerts(state="active")
        assert len(rows) == 1 and rows[0]["alert_id"] == alert["alert_id"]
        # 水位不丢：seq 5 直接触发 4 号缺口事件
        r = ingest(svc2, [sample(seq=5, value=0.1, ts=T0 + 5)])
        assert r.accepted == 1
        gaps = svc2.storage.query_events(kind="seq_gap")
        assert gaps[0]["payload"]["from_seq"] == 4
        # 去重状态不丢：重启前已见的 seq 仍判重，不重复计数
        r = ingest(svc2, [sample(seq=2, value=6.0, ts=T0 + 2)])
        assert r.duplicates == 1
        # 告警聚合在重启后续接：窗口攒够样本后，新违规并入原告警而非新开
        ingest(svc2, [sample(seq=i, value=7.0, ts=T0 + i) for i in (6, 7, 8)])
        rows = svc2.storage.query_alerts()
        assert len(rows) == 1
        assert rows[0]["violation_count"] == 6
        # 规则版本也保持热更新后的版本
        assert svc2.core.rules.version == "test-rules-1"
    finally:
        svc2.close()


def test_restart_preserves_review_queue(tmp_path, clock):
    db = tmp_path / "telemetry.db"
    svc = build(db, clock)
    svc.core.reload_rules(TEST_RULES, applied_by="test")
    ingest(svc, [sample(seq=9, ts=None)])
    svc.close()

    svc2 = build(db, clock)
    try:
        pending = svc2.storage.review_list(status="pending")
        assert len(pending) == 1
        entry = svc2.core.review_resolve(pending[0]["id"], "ingest",
                                         operator="oncall", corrected_ts=clock())
        assert entry["status"] == "ingested"
        assert svc2.storage.get_watermark("ATT.ROLL") == 9
    finally:
        svc2.close()
