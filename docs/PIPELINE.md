# Pipeline — 回测流水线

Pipeline 是 AlphaMiningV2 的回测基础设施，封装了与 WorldQuant Brain API 交互的核心逻辑。

---

## 功能概要

- **因子生成** (`main.py`) — 模板驱动的 FASTEXPR 表达式生成器，支持 Probe（代表性参数快速筛选）模式
- **回测执行** (`backtest_runner.py`) — 3 并发 worker，指数退避重试，checkpoint 续跑
- **调度决策** (`adaptive_scheduler.py`) — Probe-Expand 自适应调度，按 Sharpe/fitness/turnover 决策
- **结果分析** (`result_filter.py`) — Core 级聚合与筛选
- **数据缓存** (`datafields_store.py`) — Brain 数据字段本地缓存

### Probe-Expand 策略

先使用代表性参数快速验证核心逻辑（Probe），再对表现优异的 Core 展开全量参数网格（Expand），算力效率提升约 70%。

```
模板 + 数据集 → Probe 生成 → Probe 回测 → 调度决策 → Expand 生成 → Expand 回测 → 结果分析
                                              ├─ EXPAND
                                              ├─ WATCH
                                              └─ ABANDON
```

---

## 独立使用

### 一键运行

```bash
python run_pipeline.py --dataset-id option8 \
  --template-ids TPL_GROUP_IVHV_SMOOTH_V1 \
  --expand-min-sharpe 1.0
```

### 分阶段运行

```bash
# 1. 生成探测批次
python main.py --dataset-id option8 --template-ids ALL --probe

# 2. 执行回测
python backtest_runner.py \
  --input-dir factor_batches/probe/option8 \
  --output-dir backtest_results/probe/option8 --once

# 3. 调度决策
python adaptive_scheduler.py \
  --probe-results-dir backtest_results/probe/option8 --dataset-id option8
```

### 缓存数据字段

```bash
python pipeline/datafields_store.py --dataset-id pv1
```

---

## 模块说明

| 模块 | 功能 |
|------|------|
| `main.py` | 模板驱动的因子生成器，Probe 模式自动使用代表性值 |
| `backtest_runner.py` | Brain API 并发执行器，支持 checkpoint、自动重连 |
| `adaptive_scheduler.py` | 按 Core 聚合结果，决策 EXPAND/WATCH/ABANDON |
| `result_filter.py` | 结果聚合与多层筛选 |
| `datafields_store.py` | 数据字段抓取与分页本地缓存 |
| `run_pipeline.py` | 一键式闭环入口 |
