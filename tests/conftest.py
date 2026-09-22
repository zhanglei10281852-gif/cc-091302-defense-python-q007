import pytest

from src.service import Service, ServiceConfig
from src.engine import EngineConfig

TEST_RULES = """\
version: "test-rules-1"
defaults:
  window_sec: 60
  min_samples: 3
  cooldown_sec: 30
  resolve_sec: 30
points:
  ATT.ROLL:
    window_sec: 30
    min_samples: 3
    levels:
      warning:
        when: {abs_gt: 5.0}
        min_count: 3
      critical:
        when: {abs_gt: 10.0}
        min_count: 1
"""


class FakeClock:
    def __init__(self, now: float = 1_000_000.0):
        self._now = now

    def __call__(self) -> float:
        return self._now

    def advance(self, sec: float) -> None:
        self._now += sec


@pytest.fixture()
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture()
def service(tmp_path, clock) -> Service:
    svc = Service(
        ServiceConfig(
            db_path=str(tmp_path / "telemetry.db"),
            rules_path=None,
            engine=EngineConfig(dedup_window_sec=3600.0,
                                max_future_skew_sec=300.0,
                                max_past_skew_sec=86400.0),
        ),
        clock=clock,
    )
    svc.core.reload_rules(TEST_RULES, applied_by="test")
    yield svc
    svc.close()


def make_batch(samples, source="test"):
    return {"samples": samples, "source": source}


def sample(point_id="ATT.ROLL", seq=1, value=0.0, ts=1_000_000.0,
           config_version="cfg-A", time_quality="synced"):
    return {"point_id": point_id, "seq": seq, "value": value, "ts": ts,
            "config_version": config_version, "time_quality": time_quality}
