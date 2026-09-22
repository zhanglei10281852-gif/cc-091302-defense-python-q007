# 卫星遥测异常告警

面向卫星运维的遥测分析服务：在轨道切换等遥测突变场景下，把**通信抖动**（乱序、丢包、
重复、时间基准不可信）与**真实姿态故障**（数值越限）分离处理 —— 前者只产生数据质量
事件，后者按滑动窗口与可热更新阈值产生分级告警，避免告警风暴掩盖需要立即处置的信号。

## 运行

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python -m src.api          # 监听 0.0.0.0:8000
.venv/bin/python -m pytest           # 运行测试
```

代码位于 `src`，规则文件 `config/rules.yaml`，数据落盘 `data/telemetry.db`（SQLite，WAL）。

## 数据模型

`POST /ingest/batch` 接收批量样本：

```json
{
  "source": "gw-1",
  "samples": [{
    "point_id": "ATT.ROLL",
    "seq": 1001,
    "ts": "2026-09-22T10:00:00.123Z",
    "value": 3.2,
    "config_version": "cfg-v7",
    "time_quality": "synced"
  }]
}
```

- `ts`：ISO8601（须含时区）或 epoch 秒；缺失/不可解析/严重偏移 → **待审队列**
- `time_quality`：非 `synced/ok/good/true/locked` → 时间基准无法确认 → **待审队列**
- 响应逐条给出 `accepted / duplicate / late / review / rejected`，可定位每条数据去向

## 处理语义

| 场景 | 行为 |
|---|---|
| 乱序 | 按 (ts, seq) 插入滑动窗口，标记 `out_of_order`，正常参与统计 |
| 丢包 | seq 跳变记录 `seq_gap` 事件；迟到补齐记录 `seq_gap_filled`；水位持久化 |
| 重复 | 维护窗口内同 (point_id, seq) 只计一次（默认窗口 3600s，重启后仍有效） |
| 迟到（已滑出窗口） | 记 `late_sample` 事件留痕，不参与统计 |
| 时间基准不可确认 | 只进待审队列，绝不进入统计；值班员可修正时间后重新入库或丢弃 |
| 配置版本切换 | 窗口按 `config_version` 分段独立评估，告警分别标注切换前后版本 |

## 告警语义

- **分级**：`info / warning / critical`，按测点配置 `{条件, min_count}`；条件支持
  `gt/gte/lt/lte/abs_gt/abs_lt/outside/inside/step_gt`（突变幅度）
- **聚合防风暴**：同一 (测点, 等级, 配置版本) 的未确认告警持续累积证据
  （`violation_count`、触发样本），不重复开警
- **确认**：`POST /alerts/{id}/ack`；确认后进入冷却期（`cooldown_sec`），防止立即复发
- **自动恢复**：窗口内无违规且持续 `resolve_sec` 后自动 `resolved`
- **抑制**：`POST /suppressions` 按测点通配符 + 等级 + 时间段静默告警
  （如轨道切换维护窗口），命中告警置 `suppressed` 并留痕
- **可定位**：`GET /alerts/{id}` 返回 `trigger_samples`（seq/ts/value/配置版本）与
  `locator`（seq 与时间范围），值班员可据此检索原始帧
- 每条告警记录**规则版本**与**测点配置版本**；原始统计摘要随告警保存

## 接口一览

| 方法/路径 | 说明 |
|---|---|
| `POST /ingest/batch` · `GET /ingest/batches` | 批量接收 · 批次留痕 |
| `GET /alerts` · `GET /alerts/{id}` · `POST /alerts/{id}/ack` | 历史检索（测点/等级/状态/配置版本/时间段）· 详情 · 确认 |
| `POST /suppressions` · `GET /suppressions` · `DELETE /suppressions/{id}` | 抑制规则增查删 |
| `GET /events` | 数据质量与生命周期事件（缺口/重复/迟到/配置切换/开关警等） |
| `GET /review-queue` · `POST /review-queue/{id}/resolve` | 待审队列查询与处置（`ingest` 可带 `corrected_ts` / `drop`） |
| `GET /rules` · `PUT /rules` · `GET /rules/history` | 当前规则 · 热更新（YAML/JSON 原文）· 版本历史 |
| `GET /points` · `GET /points/{id}/window` | 测点水位/窗口概览 · 窗口快照 |
| `GET /health` | 健康检查（含当前规则版本） |

## 持久化与重启

SQLite 保存：未确认告警（重启后自动重建内存索引，聚合续接）、序列号水位、维护窗口
去重键、待审队列、抑制规则、规则版本历史、批次摘要。滑动窗口本身为内存结构
（原始帧归档不在本服务职责内），重启后随新数据重建。

## 规则热更新

`PUT /rules` 提交完整 YAML/JSON 规则即生效，无需重启；每次变更生成版本号
（未显式指定时按内容哈希），全部版本留痕可审计。见 `config/rules.yaml` 示例。
