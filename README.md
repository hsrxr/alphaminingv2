# AlphaMiningV2

面向 WorldQuant Brain 的本地因子批量生产与回测流水线。

本项目把流程拆成 3 个可独立执行的阶段：

1. 拉取并缓存数据字段。
2. 基于模板批量生成因子表达式（按批次落盘）。
3. 持续消费批次文件并提交回测，支持断点续跑与结果筛选。

## 1. 功能概览

- 模板驱动生成：从 `template_catalog.json` 读取表达式模板与槽位定义。
- 字段缓存复用：优先复用 `datafields_cache/` 中最新缓存，减少重复 API 请求。
- 可控生成规模：支持 `--max-per-template` 和 `--max-generated` 双重上限。
- 回测稳健执行：多线程提交（上限 3）、失败重试、会话定时重登录。
- 断点续跑：在 `backtest_results/checkpoints/` 保存逐因子状态，进程中断后可继续。
- 结果二次筛选：按 Sharpe/Fitness/Returns/Turnover 等指标过滤输出。

## 2. 环境要求

- Python >= 3.12
- 依赖（见 `pyproject.toml`）：
  - `requests>=2.31`
  - `pandas>=2.0`

安装依赖示例：

```bash
pip install -e .
```

或：

```bash
pip install requests pandas
```

## 3. 凭证配置

脚本会按以下顺序读取凭证：

1. 项目根目录 `.env`
2. 进程环境变量

需要提供：

- `BRAIN_USERNAME`
- `BRAIN_PASSWORD`

`.env` 示例：

```env
BRAIN_USERNAME=your_username
BRAIN_PASSWORD=your_password
```

## 4. 快速开始

### 4.1 生成因子批次

```bash
python main.py \
  --dataset-id pv13 \
  --data-type GROUP \
  --template-ids ALL \
  --batch-size 500 \
  --max-per-template 5000 \
  --max-generated 20000
```

输出位于：

- `factor_batches/<dataset>_<template-ids>/...batch_XXXX.json`

### 4.2 执行回测（单次扫描）

```bash
python backtest_runner.py --once
```

默认读取：

- 输入目录：`factor_batches/`
- 输出目录：`backtest_results/`

### 4.3 执行回测（常驻模式）

```bash
python backtest_runner.py
```

常驻模式会循环扫描新批次文件并持续处理。

### 4.4 结果筛选

```bash
python result_filter.py \
  --results-dir backtest_results \
  --status ok \
  --min-sharpe 1.5 \
  --min-fitness 1.0 \
  --max-turnover 0.7 \
  --limit 50 \
  --output backtest_results/filtered/top50.json
```

## 5. 核心脚本说明

### `main.py`

负责“拉字段 + 生成表达式 + 写批次”。

关键参数：

- `--dataset-id`：数据集 ID（默认 `pv13`）
- `--data-type`：字段类型过滤（默认 `GROUP`）
- `--template-doc`：模板目录文件（默认 `template_catalog.json`）
- `--template-ids`：模板选择（`ALL` 或逗号分隔）
- `--slot-overrides-file`：槽位覆盖配置
- `--settings-grid-file`：回测 settings 网格配置
- `--field-role-mode`：多字段角色取值模式（`auto/shared/distinguish`）
- `--batch-size`：每个输出文件的因子数量
- `--max-per-template`：每个模板最多生成多少条
- `--max-generated`：全局最多生成多少条

### `datafields_store.py`

负责分页拉取字段并按页落盘；也可单独运行。

示例：

```bash
python datafields_store.py \
  --dataset-id pv13 \
  --data-type GROUP \
  --output-dir datafields_cache
```

### `backtest_runner.py`

负责读取批次文件并提交回测。

特性：

- 并发 worker 上限自动钳制到 3（与 Brain 并发约束一致）
- 可配置重试次数与重试间隔
- 每隔一段时间自动重登录（默认约 3h50m）
- 每处理一个因子就落盘结果 + checkpoint，降低中断损失

常用参数：

- `--input-dir`
- `--output-dir`
- `--max-workers`
- `--max-retries`
- `--retry-sleep`
- `--relogin-interval-seconds`
- `--scan-interval`
- `--once`

### `result_filter.py`

负责聚合 `backtest_results/**/*.json` 的 `results`，并按阈值筛选。

常用参数：

- `--status {ok,failed}`（默认 `ok`）
- `--contains`：表达式子串匹配
- `--min-sharpe`
- `--min-fitness`
- `--min-returns`
- `--max-turnover`
- `--sort-by {sharpe,fitness,returns,turnover}`
- `--limit`
- `--output`

## 6. 配置文件说明

### `template_catalog.json`

- 定义命名规范、模板表达式、槽位、约束。
- 模板 ID 会按正则校验。

### `common_operator_slot_mappings.json`

- 定义通用 operator 槽位到算子集合的映射。
- 当模板槽位是 operator 且未显式给 `values` 时可复用该映射。

### `slot_overrides.example.json`

用于覆盖模板槽位默认值。

支持两层：

1. `global`：对所有模板生效。
2. `<template_id>`：仅对指定模板生效。

### `settings_grid.example.json`

用于批量展开 simulation settings 组合。

例如 `decay=[3,6,9]` 与 `neutralization=[MARKET,SECTOR]` 会做笛卡尔积扩展，生成多组 settings。

## 7. 目录约定

```text
alphaminingv2/
  main.py
  datafields_store.py
  backtest_runner.py
  result_filter.py
  template_catalog.json
  common_operator_slot_mappings.json
  factor_batches/
    <dataset>_<template-selection>/
      ..._batch_0001.json
  backtest_results/
    logs/
    checkpoints/
      ... (镜像 factor_batches 路径)
    ... (回测结果 json)
  datafields_cache/
    <dataset>/
      <timestamp>/
        page_0001.json
```

## 8. 断点续跑机制

- checkpoint 路径与输入批次路径保持镜像关系。
- checkpoint 中保存：因子签名、逐索引结果、更新时间。
- 若结果文件不存在但 checkpoint 存在，可从 checkpoint 恢复。
- 若 checkpoint 与当前批次签名不一致，会放弃旧状态并重新处理，避免错配。

## 9. 常见问题

### Q1: 报错缺少凭证

确认 `.env` 或环境变量里存在 `BRAIN_USERNAME` 和 `BRAIN_PASSWORD`。

### Q2: 生成太慢或文件太大

降低以下参数：

- `--template-ids`（只跑部分模板）
- `--max-per-template`
- `--max-generated`
- `--batch-size`

### Q3: 回测经常失败

可尝试：

- 降低 `--max-workers`（最小 1）
- 提高 `--max-retries`
- 增加 `--retry-sleep`

### Q4: 如何只处理新文件

常驻模式下，runner 会持续扫描输入目录；已完成批次会自动跳过。

## 10. 推荐工作流

```bash
# 1) 生成批次
python main.py --dataset-id pv13 --template-ids ALL

# 2) 单次回测（调试）
python backtest_runner.py --once --log-level INFO

# 3) 结果筛选
python result_filter.py --min-sharpe 1.5 --limit 20
```

## 11. 免责声明

本项目仅用于研究与流程自动化示例。请遵守 WorldQuant Brain 平台规则与账户条款，控制请求频率与并发，避免对服务造成不必要压力。
