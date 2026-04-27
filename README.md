# AlphaMiningV2

面向 WorldQuant Brain 的本地因子批量生产与闭环回测流水线。

AlphaMiningV2 采用了 **Probe-Expand（探测-展开）闭环架构**。系统不再机械地遍历所有参数组合，而是通过少量代表性参数（Probe）快速验证核心金融逻辑（Core）的有效性，随后自动对表现优异的 Core 进行全量参数展开（Expand），从而将算力效率提升 70% 以上。

---

## 1. 核心工作流 (The Closed-Loop Pipeline)

### 一键运行（推荐）

使用 `run_pipeline.py` 将四个阶段合并为单一命令，自动完成探测生成 → 探测回测 → 调度决策 → 扩展回测的完整闭环：

```bash
# 最简调用：使用所有默认参数
python run_pipeline.py --dataset-id option8

# 指定模板与决策阈值
python run_pipeline.py \
  --dataset-id option8 \
  --template-ids TPL_GROUP_IVHV_SMOOTH_V1 \
  --expand-min-sharpe 1.0 \
  --expand-max-turnover 0.7

# 干跑模式：仅打印调度决策，不生成扩展批次
python run_pipeline.py --dataset-id option8 --dry-run

# 仅运行探测阶段（生成 + 回测），不做调度与扩展
python run_pipeline.py --dataset-id option8 --probe-only

# 跳过已完成的阶段（如探测批次和回测结果已存在）
python run_pipeline.py --dataset-id option8 --skip-probe-gen --skip-probe-run
```

流水线完成后，使用以下命令查看 Core 级别结果摘要：

```bash
python result_filter.py --results-dir backtest_results/expand --group-by-core
```

### 各阶段单独运行（高级用法）

各阶段脚本仍可独立调用，适合调试或局部重跑：

```bash
# 阶段 1：探测批次生成
python main.py --dataset-id option8 --template-ids TPL_GROUP_IVHV_SMOOTH_V1 --probe

# 阶段 2：探测回测
python backtest_runner.py --input-dir factor_batches/probe --output-dir backtest_results/probe --once

# 阶段 3：调度决策 + 扩展批次生成
python adaptive_scheduler.py --probe-results-dir backtest_results/probe --dataset-id option8

# 阶段 4：扩展回测
python backtest_runner.py --input-dir factor_batches/expand --output-dir backtest_results/expand --once

# 结果分析
python result_filter.py --results-dir backtest_results/expand --group-by-core
```

---

## 2. 目录结构

整理后的根目录保持清爽，将文档与示例配置归类存放：

```text
alphaminingv2/
├── run_pipeline.py             # 入口：一键式闭环流水线（推荐使用）
├── main.py                     # 核心：模板驱动的因子生成器（支持 --probe 模式）
├── backtest_runner.py          # 核心：并发回测执行引擎
├── adaptive_scheduler.py       # 核心：Probe-Expand 闭环调度器
├── result_filter.py            # 核心：因子筛选与 Core 级别聚合分析
├── datafields_store.py         # 工具：数据字段缓存管理器
├── template_catalog.json       # 配置：11 个预定义因子模板及槽位约束
├── common_operator_slot_mappings.json # 配置：通用算子映射表
├── test_improvements.py        # 测试：架构改进端到端验证（33 项）
├── test_pipeline.py            # 测试：run_pipeline.py 逻辑验证（24 项）
│
├── docs/                       # 文档与架构设计报告
│   ├── Architecture_Diagnosis_and_Improvement.md
│   ├── LLM_Agent_Architecture_Design.md
│   ├── template_naming_guide.md
│   └── assets/                 # 文档配图
│
├── examples/                   # 示例配置文件
│   ├── settings_grid.example.json
│   └── slot_overrides.example.json
│
├── factor_batches/             # 生成的因子 JSON 批次（自动创建）
├── backtest_results/           # 回测结果与 Checkpoints（自动创建）
└── datafields_cache/           # 数据字段本地缓存（自动创建）
```

---

## 3. 环境要求与配置

- **Python** >= 3.12
- **依赖**：`requests>=2.31`, `pandas>=2.0`
  ```bash
  pip install requests pandas
  ```

### 凭证配置

脚本会按以下顺序读取凭证：
1. 项目根目录 `.env` 文件
2. 进程环境变量

需要提供以下变量：
```env
BRAIN_USERNAME=your_username
BRAIN_PASSWORD=your_password
```

---

## 4. 核心模块详解

### 4.1 `main.py` (Factor Generator)
负责“拉字段 + 组装表达式 + 写批次”。
- **新增特性**：支持 `--probe` 模式。开启后，数值/分类槽位将使用 `template_catalog.json` 中定义的 `representative_values`，大幅缩减无效的笛卡尔积。
- **元数据**：生成的每条记录都会携带 `core_id`（如 `iv_mean_field=iv_30d|hv_field=hv_30d`），为后续聚合提供基础。

### 4.2 `backtest_runner.py` (Execution Engine)
负责读取批次文件并并发提交给 Brain 平台。
- 并发 worker 上限严格钳制到 3，避免 HTTP 429。
- 支持常驻模式（不带 `--once`），会持续轮询 `factor_batches/` 目录的新文件。
- 每处理一个因子即落盘 checkpoint，中断后可无缝续跑。

### 4.3 `adaptive_scheduler.py` (Closed-Loop Brain)
读取 `backtest_results/probe` 中的结果，按 `core_id` 聚合计算 `sharpe_mean`、`fitness_mean` 等指标，并做出决策：
- **EXPAND**：核心逻辑有效，自动调用 `main.py` 生成该 Core 的全量参数网格。
- **WATCH**：表现平庸，记录但暂不展开。
- **ABANDON**：表现极差，直接抛弃。

### 4.4 `result_filter.py` (Result Analyzer)
除了传统的单因子指标筛选，新增了 `--group-by-core` 模式，可输出 Core 级别的表现摘要表格，帮助研究员快速定位最强信号源。

---

## 5. 架构演进与 LLM Agent 展望

本项目正从传统的“机械枚举”向“智能 Agent”架构演进。当前的 Probe-Expand 机制构成了坚实的**代码规则闭环**。

在 `docs/LLM_Agent_Architecture_Design.md` 中，我们详细规划了下一阶段的 Hybrid Agent 架构：
- **LLM Planner**：负责根据金融直觉设计新模板（Template Design）。
- **LLM Evaluator**：负责对表现平庸（WATCH）的 Core 提出变异建议（如引入非线性算子或改变中性化方式）。
- **Code Rules**：负责并发调度、字段合法性硬校验与回测执行。

---

## 6. 免责声明

本项目仅用于量化研究与流程自动化示例。请严格遵守 WorldQuant Brain 平台规则与账户条款，控制请求频率与并发，避免对服务造成不必要压力。
