"""接收链路：乱序、丢包、去重、迟到、待审队列。"""
from src.models import IngestBatchIn
from tests.conftest import make_batch, sample

T0 = 1_000_000.0


def ingest(service, samples):
    return service.core.ingest_batch(IngestBatchIn.model_validate(make_batch(samples)))


def test_out_of_order_accepted_and_watermark(service):
    # 乱序到达：seq 3 先于 2
    r = ingest(service, [
        sample(seq=1, ts=T0), sample(seq=3, ts=T0 + 2), sample(seq=2, ts=T0 + 1),
    ])
    assert r.accepted == 3
    statuses = {x.seq: x for x in r.results}
    assert statuses[3].out_of_order is False
    assert statuses[2].out_of_order is True
    # 缺口事件：2 号一度缺失，随后被补齐
    kinds = [e["kind"] for e in service.storage.query_events()]
    assert "seq_gap" in kinds
    assert "seq_gap_filled" in kinds
    assert service.storage.get_watermark("ATT.ROLL") == 3


def test_duplicate_within_window_not_counted_twice(service):
    r = ingest(service, [sample(seq=1, ts=T0), sample(seq=1, ts=T0), sample(seq=2, ts=T0 + 1)])
    assert r.accepted == 2
    assert r.duplicates == 1
    snap = service.core.window_snapshot("ATT.ROLL")
    assert len(snap["samples"]) == 2  # 重复样本未重复计数
    dup_events = service.storage.query_events(kind="duplicate")
    assert len(dup_events) == 1


def test_gap_event_for_packet_loss(service):
    ingest(service, [sample(seq=1, ts=T0)])
    ingest(service, [sample(seq=5, ts=T0 + 4)])
    gaps = service.storage.query_events(kind="seq_gap")
    assert len(gaps) == 1
    assert gaps[0]["payload"]["from_seq"] == 2
    assert gaps[0]["payload"]["to_seq"] == 4
    assert gaps[0]["payload"]["gap_len"] == 3


def test_late_sample_outside_window_only_logged(service):
    ingest(service, [sample(seq=10, ts=T0)])
    # 窗口 30s：该样本比窗口锚点早 40s，已滑出维护窗口
    r = ingest(service, [sample(seq=2, ts=T0 - 40)])
    assert r.late == 1
    assert r.results[0].status == "late"
    snap = service.core.window_snapshot("ATT.ROLL")
    assert all(s["seq"] != 2 for s in snap["samples"])
    assert service.storage.query_events(kind="late_sample")


def test_missing_timestamp_goes_to_review_queue(service):
    r = ingest(service, [sample(seq=1, ts=None)])
    assert r.review == 1
    entries = service.storage.review_list(status="pending")
    assert len(entries) == 1
    assert entries[0]["reason"] == "timestamp_missing_or_unparseable"
    # 待审数据不进入窗口统计
    assert service.core.window_snapshot("ATT.ROLL") is None


def test_unconfirmed_time_quality_goes_to_review(service):
    r = ingest(service, [sample(seq=1, ts=T0, time_quality="free-running")])
    assert r.review == 1
    entry = service.storage.review_list(status="pending")[0]
    assert entry["reason"] == "time_quality_unconfirmed"


def test_future_timestamp_goes_to_review(service, clock):
    r = ingest(service, [sample(seq=1, ts=clock() + 3600)])
    assert r.review == 1
    assert service.storage.review_list(status="pending")[0]["reason"] == "future_timestamp"


def test_review_resolve_ingest_with_corrected_ts(service, clock):
    ingest(service, [sample(seq=7, ts=None)])
    rid = service.storage.review_list(status="pending")[0]["id"]
    entry = service.core.review_resolve(rid, "ingest", operator="oncall",
                                        corrected_ts=clock())
    assert entry["status"] == "ingested"
    snap = service.core.window_snapshot("ATT.ROLL")
    assert [s["seq"] for s in snap["samples"]] == [7]
    assert service.storage.get_watermark("ATT.ROLL") == 7


def test_review_resolve_drop(service):
    ingest(service, [sample(seq=7, ts="not-a-time")])
    rid = service.storage.review_list(status="pending")[0]["id"]
    entry = service.core.review_resolve(rid, "drop", operator="oncall")
    assert entry["status"] == "dropped"
    assert service.core.window_snapshot("ATT.ROLL") is None


def test_batch_summary_persisted(service):
    r = ingest(service, [sample(seq=1, ts=T0), sample(seq=1, ts=T0), sample(seq=2, ts=None)])
    batches = service.storage.batch_list()
    assert len(batches) == 1
    summary = batches[0]["summary"]
    assert summary["total"] == 3
    assert summary["accepted"] == 1
    assert summary["duplicate"] == 1
    assert summary["review"] == 1
    assert batches[0]["batch_id"] == r.batch_id
