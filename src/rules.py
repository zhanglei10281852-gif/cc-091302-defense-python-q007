"""阈值规则：滑动窗口 + 分级阈值，支持热更新与版本留痕。

规则示例 (YAML)::

    version: "2026.09.22-a"          # 可选；缺省时按内容哈希生成
    defaults:
      window_sec: 60
      min_samples: 3                 # 窗口段内至少这么多有效样本才评估
      cooldown_sec: 60               # 告警自动恢复后再次触发的冷却
      resolve_sec: 60                # 无新增违规多久后自动恢复
    points:
      ATT.ROLL:
        window_sec: 30
        levels:
          warning:
            when: {abs_gt: 5.0}
            min_count: 3             # 窗口段内违规样本数达到即触发
          critical:
            when: {abs_gt: 10.0}
            min_count: 1

条件 (when) 支持: gt/gte/lt/lte/abs_gt/abs_lt、outside: [lo, hi]、
inside: [lo, hi]、step_gt: x（与前一有效样本的突变幅度）。
"""
from __future__ import annotations

import hashlib
from typing import Any, Optional

import yaml
from pydantic import BaseModel, Field, model_validator

from .models import Level


class Condition(BaseModel):
    gt: Optional[float] = None
    gte: Optional[float] = None
    lt: Optional[float] = None
    lte: Optional[float] = None
    abs_gt: Optional[float] = None
    abs_lt: Optional[float] = None
    outside: Optional[list[float]] = None   # [lo, hi] 之外为违规
    inside: Optional[list[float]] = None    # [lo, hi] 之内为违规
    step_gt: Optional[float] = None         # 相邻样本突变幅度

    @model_validator(mode="after")
    def _non_empty(self) -> "Condition":
        if not any(getattr(self, f) is not None for f in type(self).model_fields):
            raise ValueError("条件不能为空")
        return self

    def matches(self, value: float, prev: Optional[float]) -> bool:
        c = self
        if c.gt is not None and value > c.gt:
            return True
        if c.gte is not None and value >= c.gte:
            return True
        if c.lt is not None and value < c.lt:
            return True
        if c.lte is not None and value <= c.lte:
            return True
        if c.abs_gt is not None and abs(value) > c.abs_gt:
            return True
        if c.abs_lt is not None and abs(value) < c.abs_lt:
            return True
        if c.outside is not None:
            lo, hi = c.outside
            if value < lo or value > hi:
                return True
        if c.inside is not None:
            lo, hi = c.inside
            if lo <= value <= hi:
                return True
        if c.step_gt is not None and prev is not None and abs(value - prev) > c.step_gt:
            return True
        return False


class LevelRule(BaseModel):
    when: Condition
    min_count: int = 1


class PointRule(BaseModel):
    window_sec: Optional[float] = None
    min_samples: Optional[int] = None
    cooldown_sec: Optional[float] = None
    resolve_sec: Optional[float] = None
    levels: dict[str, LevelRule] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _levels_valid(self) -> "PointRule":
        for name in self.levels:
            Level.parse(name)  # 非法等级名直接报错
        return self


class Defaults(BaseModel):
    window_sec: float = 60.0
    min_samples: int = 3
    cooldown_sec: float = 60.0
    resolve_sec: float = 60.0


class RuleSet(BaseModel):
    version: str = ""
    defaults: Defaults = Field(default_factory=Defaults)
    points: dict[str, PointRule] = Field(default_factory=dict)

    def point(self, point_id: str) -> Optional[PointRule]:
        return self.points.get(point_id)

    def window_sec(self, point_id: str) -> float:
        r = self.point(point_id)
        return (r.window_sec if r and r.window_sec else self.defaults.window_sec)

    def min_samples(self, point_id: str) -> int:
        r = self.point(point_id)
        return (r.min_samples if r and r.min_samples else self.defaults.min_samples)

    def cooldown_sec(self, point_id: str) -> float:
        r = self.point(point_id)
        return (r.cooldown_sec if r and r.cooldown_sec is not None else self.defaults.cooldown_sec)

    def resolve_sec(self, point_id: str) -> float:
        r = self.point(point_id)
        return (r.resolve_sec if r and r.resolve_sec is not None else self.defaults.resolve_sec)


def parse_ruleset(content: str) -> RuleSet:
    """解析 YAML/JSON 规则文本；缺省版本号时按内容哈希生成。"""
    data = yaml.safe_load(content)
    if not isinstance(data, dict):
        raise ValueError("规则内容必须是映射结构")
    rs = RuleSet.model_validate(data)
    if not rs.version:
        digest = hashlib.sha256(content.encode("utf-8")).hexdigest()[:12]
        rs.version = f"r-{digest}"
    return rs
