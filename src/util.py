"""时间、数值与匹配工具。"""
from __future__ import annotations

import fnmatch
import math
import time
from datetime import datetime, timezone
from typing import Optional


def now_ms() -> int:
    return int(time.time() * 1000)


def parse_iso(value: Optional[str]) -> Optional[int]:
    """ISO8601 -> epoch 毫秒。无法解析返回 None。"""
    if not value:
        return None
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def to_iso(ms: Optional[int]) -> Optional[str]:
    if ms is None:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def is_finite_number(x) -> bool:
    return isinstance(x, (int, float)) and not isinstance(x, bool) and math.isfinite(x)


def point_matches(pattern: str, point_id: str) -> bool:
    """维护窗口的测点匹配：* 全通配，否则精确。"""
    if pattern == "*":
        return True
    if "*" in pattern or "?" in pattern or "[" in pattern:
        return fnmatch.fnmatchcase(point_id, pattern)
    return pattern == point_id


def overlaps(a_start: int, a_end: int, b_start: int, b_end: int) -> bool:
    return a_start <= b_end and b_start <= a_end
