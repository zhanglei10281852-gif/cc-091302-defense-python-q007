"""请求/响应数据模型。"""
from __future__ import annotations

from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field

TimeQuality = Literal["good", "uncertain", "bad"]
Severity = Literal["warning", "critical"]


class SampleIn(BaseModel):
    model_config = ConfigDict(allow_nan=False)

    seq: int = Field(..., ge=0, description="序列号，按卫星单调递增")
    point_id: str
    value: float
    sample_time: Optional[str] = Field(None, description="ISO8601 采样时间；缺失进入待审队列")
    config_version: Optional[str] = Field(None, description="上送方测点配置版本")
    time_quality: TimeQuality = "good"


class BatchIn(BaseModel):
    satellite_id: str
    samples: list[SampleIn]
    batch_id: Optional[str] = None


class AckIn(BaseModel):
    operator: str
    note: Optional[str] = None


class ResolvePendingIn(BaseModel):
    action: Literal["accept", "reject"]
    corrected_time: Optional[str] = None
    note: Optional[str] = None


class SuppressionIn(BaseModel):
    satellite_id: str
    point_id: str = Field("*", description="测点 id，支持 * 通配")
    severities: list[Severity] = Field(default_factory=lambda: ["warning", "critical"])
    start_time: str
    end_time: str
    reason: str = ""


class RuleSetIn(BaseModel):
    version: str
    rules: dict[str, dict]
    activate: bool = True
