# 卫星遥测异常告警服务

轨道切换期间遥测突变频发。本服务把**通信抖动**（孤立尖峰、乱序、重发）与
**真正的姿态故障**（滑动窗口内连续越阈值）区分开，按阈值产生分级告警并全程留痕，
避免告警风暴淹没需要立即处置的信号。

运行环境：Python 3.11，FastAPI + SQLite(WAL)，无外部服务依赖。

## 快速开始

```bash
python -m venv .venv && .venv/bin/pip install -r requirements.txt
python -m src.service                 # 默认 0.0.0.0:8080
# 或：PORT=8080 TELEMETRY_DB=data/telemetry.db TELEMETRY_RULES=config/rules.json
# Swagger 文档：http://localhost:8080/docs
pytest -q
```

## 核心语义

| 需求 | 实现 |
|---|---|
| 乱序、丢包 | 每星维护序列号**高水位 + 缺口表**；迟到样本正常入窗并回填缺口，批次返回 `high_water / missing_sequences / out_of_order / gap_filled`。首点不臆造缺口 |
| 抖动 vs 故障 | **滑动时间窗口 m-of-n**：窗口内仅当越界样本数达到 `warning_count/critical_count` 才升级；单点尖峰不产生告警。支持范围 `min/max`、跳变 `max_delta`、恢复迟滞 `hysteresis` |
| 分级告警 | `warning` → `critical` 自动升级；持续恢复后自动 `closed(recovered)` |
| 维护窗口重复样本不重复计数 | `(satellite_id, seq, point_id)` 主键去重；`batch_id` 整体幂等回放。重复样本返回 `duplicate`，不进入窗口统计 |
| 阈值热更新 | 版本化规则集：`POST /api/v1/rules` 即时生效，或修改 `config/rules.json`（2s mtime 轮询，可 `POST /api/v1/rules/reload` 强制加载；坏文件保留现行规则） |
| 切换前后分别标注 | 样本与告警永久携带 `rule_version`（规则版本）与 `config_version`（上送方测点配置版本）；切换时旧版本未结告警置 `config_switch=1`、按 `rule_switch` 关闭归档，新版本样本产生独立告警，响应中 `config_phase=before_switch/current` |
| 时间基准不可确认 | 时间缺失/无法解析、`time_quality=bad|uncertain`、超过钟偏（默认 60s）的未来时间一律进入**待审队列**，不参与评估；可 `accept`（给修正时间）或 `reject` |
| 告警确认 | `POST /alerts/{id}/ack`，记录值班员、备注、时间，幂等，重启不丢 |
| 抑制（维护窗口） | 按卫星/测点通配符（如 `attitude.*`）/级别/时间段匹配；事后下发立即抑制既有活动告警；抑制告警**仍留痕可查**（`status=suppressed`，带 `suppression_id`），窗口到期/删除后自动恢复可见 |
| 历史检索 | 告警多条件过滤（星、测点、级别、状态、规则版本、切换标记、时间）；样本与原始批次摘要（payload sha256、规则版本、逐样本结论）均可回溯 |
| 定位触发样本 | 每条告警返回 `trigger_samples`（值/时间/违规类型）与 `trigger_sample_refs[].locator`，格式 `SAT/point#seq` |
| 重启不丢 | SQLite 持久化告警（含未确认）、确认关系、序列号水位/缺口、规则版本、待审队列、批次摘要；WAL + 批次级事务 |

### 规则文件格式（`config/rules.json`）

```json
{
  "version": "rules-2026-09-21-01",
  "rules": {
    "attitude.roll_deg": {"min": -35, "max": 35, "window_sec": 30,
                          "warning_count": 2, "critical_count": 4, "hysteresis": 1.0},
    "attitude.yaw_rate_dps": {"max_delta": 5.0, "window_sec": 10,
                              "warning_count": 2, "critical_count": 3}
  }
}
```
测点支持 `"*"` 兜底规则；不配置 `critical_count` 表示该测点永不升 critical。

## 接口一览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/v1/telemetry/batch` | 批量接收样本，返回水位/缺口/去重/待审/告警变化 |
| GET  | `/api/v1/telemetry/pending` | 待审队列（`status=open/accepted/rejected`） |
| POST | `/api/v1/telemetry/pending/{id}/resolve` | 待审处置 `accept/reject` |
| GET  | `/api/v1/streams` | 序列号水位与缺口 |
| GET  | `/api/v1/alerts` | 告警检索（支持多值 `severity/status`、`config_switch`、`rule_version`、时间窗） |
| GET  | `/api/v1/alerts/{id}` | 告警详情（含触发样本定位） |
| POST | `/api/v1/alerts/{id}/ack` | 告警确认 |
| POST/GET/DELETE | `/api/v1/suppressions[/{id}]` | 维护窗口抑制管理（`active_only=true`） |
| GET  | `/api/v1/history/samples` | 原始样本检索 |
| GET  | `/api/v1/history/batches/{batch_id}` | 批次原始摘要（sha256 + 规则版本） |
| GET/POST | `/api/v1/rules`、`POST /api/v1/rules/{version}/activate`、`POST /api/v1/rules/reload` | 规则版本与热更新 |

### 批次请求示例

```json
{
  "satellite_id": "SAT-ORBIT-7",
  "batch_id": "orbit-batch-1",
  "samples": [
    {"seq": 6, "point_id": "attitude.roll_deg", "value": 42.0,
     "sample_time": "2026-09-21T14:13:21Z",
     "config_version": "cfg-A", "time_quality": "good"}
  ]
}
```

## 结构

```
src/models.py    Pydantic 入参模型
src/storage.py   SQLite schema 与连接（WAL，进程锁串行化写入）
src/engine.py    序列号水位/缺口、去重、滑窗评估、告警生命周期、规则切换、抑制、待审
src/app.py       FastAPI 路由 + 规则文件 mtime 热加载线程
src/service.py   启动入口
config/rules.json 示例规则（部署时可替换，支持热更新）
data/            SQLite 数据目录（git 忽略）
tests/           端到端测试（12 项，含模拟重启持久化）
```

并发模型：所有引擎方法在进程内 `RLock` 下执行，批次以 `BEGIN IMMEDIATE`
事务原子提交；待审处置后重新走完整接收管线。
