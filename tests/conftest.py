import os
import sys
import tempfile
from pathlib import Path

import pytest

# 在导入应用前指向临时库与规则文件
_TMP = Path(tempfile.mkdtemp(prefix="telemetry-test-"))
os.environ["TELEMETRY_DB"] = str(_TMP / "test.db")
os.environ["TELEMETRY_RULES"] = str(_TMP / "rules.json")
os.environ["TELEMETRY_RULES_POLL"] = "3600"
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi.testclient import TestClient  # noqa: E402

from src.app import app  # noqa: E402


@pytest.fixture(scope="session")
def tmp_root() -> Path:
    return _TMP


@pytest.fixture()
def client():
    with TestClient(app) as c:
        yield c


@pytest.fixture()
def rules_file(tmp_root) -> Path:
    return Path(os.environ["TELEMETRY_RULES"])


@pytest.fixture()
def db_path() -> str:
    return os.environ["TELEMETRY_DB"]
