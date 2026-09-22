"""卫星遥测异常告警服务入口（组合根）。"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Callable, Optional

from .engine import EngineConfig, TelemetryCore
from .rules import RuleSet, parse_ruleset
from .storage import Storage

DEFAULT_RULES = """\
version: "builtin-default-1"
defaults:
  window_sec: 60
  min_samples: 3
  cooldown_sec: 60
  resolve_sec: 60
points: {}
"""


@dataclass
class ServiceConfig:
    db_path: str = "data/telemetry.db"
    rules_path: Optional[str] = "config/rules.yaml"
    engine: EngineConfig = None  # type: ignore[assignment]

    def __post_init__(self) -> None:
        if self.engine is None:
            self.engine = EngineConfig()


class Service:
    """领域服务的基础入口：装配存储、规则与核心引擎。"""

    def __init__(self, config: Optional[ServiceConfig] = None,
                 clock: Callable[[], float] = time.time):
        self.config = config or ServiceConfig()
        db_dir = os.path.dirname(self.config.db_path)
        if db_dir:
            os.makedirs(db_dir, exist_ok=True)
        self.storage = Storage(self.config.db_path)
        self.core = TelemetryCore(self.storage, self._load_rules(),
                                  config=self.config.engine, clock=clock)
        self.ready = True

    def _load_rules(self) -> RuleSet:
        # 优先使用库中最近一次的规则版本（热更新结果重启后仍然生效）
        latest = self.storage.rules_latest()
        if latest:
            return parse_ruleset(latest["content"])
        if self.config.rules_path and os.path.exists(self.config.rules_path):
            with open(self.config.rules_path, "r", encoding="utf-8") as f:
                content = f.read()
            rs = parse_ruleset(content)
            self.storage.rules_save(rs.version, content, time.time(), "bootstrap")
            return rs
        rs = parse_ruleset(DEFAULT_RULES)
        self.storage.rules_save(rs.version, DEFAULT_RULES, time.time(), "bootstrap")
        return rs

    def close(self) -> None:
        self.ready = False
        self.storage.close()
