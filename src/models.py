"""遥测告警领域模型。

约定:
- 所有时间在内部一律使用 UTC epoch 秒 (float)，对外序列化为 ISO8601 UTC 字符串。
- 样本唯一键为 (point_id, seq)，用于维护窗口内去重。
"""
from __future__ import annotations

import math
from datetime import datetime, timezone
from enum import IntEnum
from typing import Any, Optional

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# 时间工具
# ---------------------------------------------------------------------------

def to_epoch(ts: Any) -> Optional[float]:
    """把 ISO8601 字符串 / epoch 秒(数值) 转成 UTC epoch 秒；无法解析返回 None。"""
    if ts is None:
        return None
    if isinstance(ts, bool):
        return None
    if isinstance(ts, (int, float)):
        if not math.isfinite(float(ts)):
            return None
        return float(ts)
    if isinstance(ts, datetime):
        dt = ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    if isinstance(ts, str):
        text = ts.strip()
        if not text:
            return None
        try:
            return float(text)
        except ValueError:
            pass
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00").replace("z", "+00:00"))
        except ValueError:
            return None
        if dt.tzinfo is None:
            return None  # 无时区信息 → 时间基准无法确认
        return dt.timestamp()
    return None


def iso(epoch: Optional[float]) -> Optional[str]:
    if epoch is None:
        return None
    return datetime.fromtimestamp(epoch, tz=timezone.utc).isoformat().replace("+00:00", "Z")


# ---------------------------------------------------------------------------
# 告警分级
# ---------------------------------------------------------------------------

class Level(IntEnum):
    info = 1
    warning = 2
    critical = 3

    @classmethod
    def parse(cls, v: Any) -> "Level":
        if isinstance(v, Level):
            return v
        return cls[str(v).strip().lower()]


class AlertState(str):
    ACTIVE = "active"            # 未确认
    ACKNOWLEDGED = "acknowledged"
    SUPPRESSED = "suppressed"
    RESOLVED = "resolved"


# ---------------------------------------------------------------------------
# 入站样本
# ---------------------------------------------------------------------------

# 被视为"时间基准可信"的标记取值
CONFIRMED_TIME_QUALITY = {"synced", "ok", "good", "true", "locked"}


class SampleIn(BaseModel):
    """批量接收接口中的单条遥测样本。"""
    point_id: str = Field(min_length=1)
    seq: int
    value: float
    ts: Any = None                      # ISO8601 或 epoch 秒；缺失/不可解析 → 待审
    config_version: str = "unknown"     # 测点配置版本
    time_quality: str = "synced"        # 时间基准质量标记

    @field_validator("point_id", "config_version", mode="before")
    @classmethod
    def _strip(cls, v: Any) -> Any:
        return v.strip() if isinstance(v, str) else v

    def epoch(self) -> Optional[float]:
        return to_epoch(self.ts)

    def time_confirmed(self) -> bool:
        return str(self.time_quality).strip().lower() in CONFIRMED_TIME_QUALITY


class IngestBatchIn(BaseModel):
    samples: list[SampleIn] = Field(min_length=1, max_length=10000)
    source: str = "unknown"             # 数据来源标识，用于留痕


class SampleResult(BaseModel):
    """单条样本的处理结果，值班员可据此定位每条数据的去向。"""
    point_id: str
    seq: int
    status: str                          # accepted / duplicate / late / review / rejected
    detail: str = ""
    out_of_order: bool = False
    ts: Optional[str] = None


class BatchResult(BaseModel):
    batch_id: str
    received_at: str
    total: int
    accepted: int
    duplicates: int
    late: int
    review: int
    rejected: int
    results: list[SampleResult]


# ---------------------------------------------------------------------------
# 告警
# ---------------------------------------------------------------------------

class TriggerSample(BaseModel):
    """触发告警的样本摘要 —— 值班员凭 (point_id, seq, ts) 定位原始数据。"""
    point_id: str
    seq: int
    ts: Optional[str]
    value: float
    config_version: str


class Alert(BaseModel):
    alert_id: str
    point_id: str
    level: str
    state: str = AlertState.ACTIVE
    rule_version: str                    # 产生该告警的规则版本
    config_version: str                  # 触发样本所属的测点配置版本（切换前后分别标注）
    opened_at: str
    updated_at: str
    resolved_at: Optional[str] = None
    ack_by: Optional[str] = None
    ack_at: Optional[str] = None
    ack_note: Optional[str] = None
    suppressed_by: Optional[str] = None  # 命中抑制规则 ID
    violation_count: int = 0             # 窗口内累计违规样本数（去重后）
    message: str = ""
    stats: dict[str, Any] = Field(default_factory=dict)   # 窗口统计快照（原始摘要）
    locator: dict[str, Any] = Field(default_factory=dict)  # seq/ts 范围，便于检索原始帧
    trigger_samples: list[TriggerSample] = Field(default_factory=list)
