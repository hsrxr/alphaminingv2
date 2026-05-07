# AlphaMiningV2

面向 WorldQuant Brain 的本地因子批量生产与闭环回测流水线，以及基于 LLM 的全自主因子搜索 Agent。

---

## 1. 系统架构

AlphaMiningV2 包含两套子系统，可独立或协同工作：

### Pipeline — Probe-Expand 闭环流水线

通过**探测-展开（Probe-Expand）**架构高效验证因子：先用代表性参数（Probe）快速筛选核心逻辑（Core），再对表现优异的 Core 展开全量参数网格（Expand），算力效率提升 70% 以上。

```
模板 + 数据集 → Probe 生成 → Probe 回测 → 调度决策 → Expand 生成 → Expand 回测 → 结果分析
                                              ├─ EXPAND
                                              ├─ WATCH
                                              └─ ABANDON
```

### Agent — 全自主因子搜索 Agent

在 Pipeline 之上构建的 LLM 驱动智能体，实现 **Idea → Template → Probe → Analyze → Decide** 完整闭环：

```
金融直觉 (Idea)
    ↓
[LLM] 生成因子模板
    ↓
[Pipeline] Probe 生成 + 回测
    ↓
[LLM] 分析结果，诊断问题
    ↓
  ┌─ EXPAND   → 全量参数网格回测
  ├─ MUTATE   → 变异表达式 → 重新 Probe
  ├─ ABANDON  → 记录并放弃
  └─ FINALIZE → 报告优秀因子
    ↑______________|  (循环直至收敛)
```

---

## 2. 目录结构

```
alphaminingv2/
├── pipeline/                       # Probe-Expand 流水线核心
│   ├── main.py                     #   模板驱动因子生成器（支持 --probe）
│   ├── backtest_runner.py          #   并发回测执行引擎（Brain API）
│   ├── adaptive_scheduler.py       #   Probe-Expand 闭环调度器
│   ├── result_filter.py            #   结果筛选与 Core 聚合分析
│   ├── datafields_store.py         #   数据字段缓存管理器
│   └── run_pipeline.py             #   一键式闭环流水线入口
│
├── agent/                          # LLM 驱动因子搜索 Agent
│   ├── wq_tools.py                 #   WQ Brain 工具层（16 个工具）
│   ├── direct_agent.py             #   无模板 Direct Agent（Phase 4）
│   ├── orchestrator.py             #   主循环：全自主搜索（Phase 3，模板式）
│   ├── feedback_loop.py            #   交互式 Agent 循环（Phase 1）
│   ├── llm_client.py               #   DeepSeek API 客户端
│   ├── expression_validator.py     #   WQ 表达式语法校验
│   ├── template_generator.py       #   金融直觉 → 因子模板（Phase 2）
│   ├── mutation_engine.py          #   因子变异引擎（规则 + LLM）
│   ├── result_analyzer.py          #   回测结果 LLM 深度分析
│   ├── convergence.py              #   收敛检测与停止决策
│   └── memory.py                   #   因子迭代记忆与持久化
│
├── adaptive_scheduler.py           # ┐
├── backtest_runner.py              # ├ 根层封装，委托给 pipeline/
├── datafields_store.py             # │ 保持向后兼容
├── main.py                         # │
├── result_filter.py                # │
├── run_pipeline.py                 # ┘
│
├── template_catalog.json           # 配置：预定义因子模板及槽位约束
├── template_catalog_mixed.json     # 配置：多数据集混合模板库
├── common_operator_slot_mappings.json # 配置：通用算子映射表
├── wq_operators_cleaned.json       # 配置：50 个 WQ 算子定义
├── .env.example                    # 环境变量模板
│
├── test_pipeline.py                # Pipeline 逻辑测试（24 项）
├── test_improvements.py            # 架构改进端到端验证（33 项）
├── run_all_new_templates.sh        # 批量运行全部模板的脚本
│
├── docs/                           # 文档
│   ├── LLM_Agent_Architecture_Design.md
│   ├── Architecture_Diagnosis_and_Improvement.md
│   └── template_naming_guide.md
│
├── agent_output/                   # Agent 会话输出（自动创建）
├── factor_batches/                 # 因子 JSON 批次（自动创建）
├── backtest_results/               # 回测结果与 Checkpoints（自动创建）
├── datafields_cache/               # 数据字段本地缓存（自动创建）
└── pipeline_logs/                  # 流水线日志（自动创建）
```

---

## 3. Pipeline 快速开始

### 一键运行（推荐）

```bash
# 最简调用
python run_pipeline.py --dataset-id option8

# 指定模板与决策阈值
python run_pipeline.py \
  --dataset-id option8 \
  --template-ids TPL_GROUP_IVHV_SMOOTH_V1 \
  --expand-min-sharpe 1.0 \
  --expand-max-turnover 0.7

# 干跑模式：仅打印调度决策
python run_pipeline.py --dataset-id option8 --dry-run

# 仅运行探测阶段
python run_pipeline.py --dataset-id option8 --probe-only

# 跳过已完成阶段
python run_pipeline.py --dataset-id option8 --skip-probe-gen --skip-probe-run
```

### 分阶段运行

```bash
# 阶段 1：探测批次生成
python main.py --dataset-id option8 --template-ids ALL --probe

# 阶段 2：探测回测
python backtest_runner.py --input-dir factor_batches/probe/option8 --output-dir backtest_results/probe/option8 --once

# 阶段 3：调度决策 + 扩展批次生成
python adaptive_scheduler.py --probe-results-dir backtest_results/probe/option8 --dataset-id option8

# 阶段 4：扩展回测
python backtest_runner.py --input-dir factor_batches/expand/option8 --output-dir backtest_results/expand/option8 --once

# 结果分析
python result_filter.py --results-dir backtest_results/expand/option8 --group-by-core
```

### 批量运行所有模板

```bash
bash run_all_new_templates.sh
```

---

## 4. Agent 快速开始

### 交互式 Agent 循环（Phase 1）

加载已完成的 Probe 结果，逐 Core 展示 LLM 诊断并接受决策指令：

```bash
python -m agent.feedback_loop \
  --probe-results-dir backtest_results/probe/option8 \
  --dataset-id option8
```

自动模式（不等待交互）：

```bash
python -m agent.feedback_loop \
  --probe-results-dir backtest_results/probe/option8 \
  --dataset-id option8 \
  --auto
```

### Direct Agent — 无模板直接因子挖掘（Phase 4）

LLM 通过工具调用直接探索数据集、字段、算子，构造 FASTEXPR 表达式并提交回测，无需预定义模板。始终保持 3 个并发回测（Brain 限制），事件驱动迭代：

```bash
# 从金融直觉出发（推荐）
python -m agent.direct_agent \
  --dataset-id pv13 \
  --idea "Momentum combined with low volatility" \
  --iterations 10

# 改进现有表达式
python -m agent.direct_agent \
  --dataset-id pv13 \
  --expression "group_rank(ts_mean(returns,21),industry)" \
  --critique "High turnover, try longer lookback"

# 设置收敛目标
python -m agent.direct_agent \
  --dataset-id pv13 \
  --idea "Mean reversion in weekly returns" \
  --target-sharpe 2.0 \
  --iterations 20
```

Agent 流程：

```
Research Phase: LLM calls tools to explore fields, operators, settings
      ↓ (type: "submit")
3 concurrent backtests on Brain API
      ↓ (poll every 30s, any completed triggers analysis)
Analysis Phase: LLM reviews sharpe/turnover/fitness
      ├─ improvements → resubmit (replace completed slot)
      ├─ knowledge    → save to knowledge base
      └─ converged    → finish
      ↑__________________|  (loop)
```

### 全自主因子搜索 Agent（Phase 3 — 模板式）

从金融直觉出发，自动完成模板生成 → Probe → 分析 → 变异/扩展的全循环：

```bash
# 需要设置 DEEPSEEK_API_KEY 环境变量或 .env 文件
python -m agent.orchestrator \
  --dataset-id option8 \
  --idea "IV skew minus HV skew for volatility risk premium" \
  --iterations 5
```

使用现有模板（跳过生成阶段）：

```bash
python -m agent.orchestrator \
  --dataset-id option8 \
  --template-path agent_output/<session_id>/templates/catalog_TPL_xxx.json \
  --iterations 5
```

### Agent 输出结构

```
agent_output/<session_id>/
├── templates/                  # 生成的模板与变异模板
│   ├── catalog_TPL_xxx.json    #   初始模板目录
│   └── mutations_round_N.json  #   第 N 轮的变异模板
├── batches/                    # 因子批次
│   ├── probe_round_1/          #   每轮 Probe 批次
│   ├── probe_round_2/
│   └── expand/                 #   Expand 批次
├── results/                    # 回测结果
│   ├── probe_round_1/
│   ├── probe_round_2/
│   └── expand/
├── memory/                     # 会话状态（可恢复）
│   └── session_<id>.json
└── report.json                 # 完整会话报告
```

---

## 5. 环境要求与配置

- **Python** >= 3.12
- **依赖**：`requests>=2.31`, `pandas>=2.0`
  ```bash
  pip install requests pandas
  ```

### 凭证配置

按以下优先级读取：

1. 项目根目录 `.env` 文件
2. 进程环境变量

| 变量 | 用途 | 必需 |
|------|------|------|
| `BRAIN_USERNAME` | WorldQuant Brain 用户名 | Pipeline |
| `BRAIN_PASSWORD` | WorldQuant Brain 密码 | Pipeline |
| `DEEPSEEK_API_KEY` | DeepSeek API 密钥 | Agent |

### 数据集

本地已缓存以下数据集字段定义：

| 数据集 | 字段数 | 用途 |
|--------|--------|------|
| `option8` | 64 | 期权 IV/HV |
| `pv1` | 135 | 价格与成交量 |
| `fundamental6` | 574 | 基本面 |
| `sentiment1` | 17 | 情绪 |
| `news12` | 75 | 新闻 |
| `analyst4` | 469 | 分析师预期 |
| `option9` | - | 期权 PCR |
| `socialmedia12` | - | 社交媒体 |

---

## 6. 核心模块详解

### 6.1 `pipeline/main.py` — Factor Generator

模板驱动的因子表达式生成器。
- **Probe 模式** (`--probe`)：数值/分类槽位使用 `representative_values`，大幅缩减笛卡尔积
- **Core ID**：每条记录携带 `pipeline_core_id`（如 `iv_mean_field=iv_30d|hv_field=hv_30d`），用于聚合
- **元数据**：自动附加 `template_id`、`run_label`、时间戳

### 6.2 `pipeline/backtest_runner.py` — Execution Engine

WorldQuant Brain 并发回测执行引擎。
- 最大 3 并发 worker（Brain API 限制）
- 指数退避重试（401/403/408/409/425/429/5xx）
- 周期性重登录（13800s）
- Checkpoint 机制，中断后无缝续跑

### 6.3 `pipeline/adaptive_scheduler.py` — Closed-Loop Scheduler

按 `core_id` 聚合 Probe 结果，以 Sharpe / fitness / turnover 为阈值决策：
- **EXPAND** (Sharpe ≥ 0.8)：自动调用 `main.py` 生成全量参数网格
- **WATCH** (Sharpe ≥ 0.5)：记录但暂不展开
- **ABANDON** (Sharpe < 0.5)：直接抛弃

### 6.4 `agent/` — LLM 驱动智能体

| 模块 | 功能 |
|------|------|
| `llm_client.py` | DeepSeek API 封装，支持 `chat()` 与 `chat_structured()` |
| `expression_validator.py` | 校验 WQ FASTEXPR 语法、算子名、字段引用 |
| `template_generator.py` | 金融直觉 → 带槽位的模板（含验证与持久化） |
| `mutation_engine.py` | 12 种规则变异 + LLM 创意变异 |
| `result_analyzer.py` | 加载回测结果，LLM 逐 Core 诊断 |
| `convergence.py` | 收敛检测：超参配置最大轮次/耐心值/优秀 Sharpe 阈值 |
| `memory.py` | 全量状态持久化，支持断点恢复 |
| `wq_tools.py` | WQ Brain 工具层（16 个工具）：字段/算子查询、回测提交轮询、知识库 |
| `direct_agent.py` | 无模板 Direct Agent：LLM 通过工具调用直接构造表达式，3 并发事件驱动 |
| `orchestrator.py` | 主循环编排：生成 → 回测 → 分析 → 决策（模板式） |
| `feedback_loop.py` | 交互式诊断界面 |

---

## 7. Agent 决策流程

每轮迭代中，对每个 Core 执行以下分类：

```
                    Sharpe ≥ 1.5? ───→ FINALIZE
                          ↓
              0.8 ≤ Sharpe < 1.5?    且 turnover ≤ 0.7, fitness ≥ 0.3?
                    ├─ yes → EXPAND
                    └─ no  → continue
                          ↓
              0.3 ≤ Sharpe < 0.8? ───→ MUTATE → 生成变异 → 下一轮 Probe
                          ↓
                    Sharpe < 0.3? ───→ ABANDON
```

检测到以下任一条件时停止：
- Core 达到 `max_rounds`（默认 5）且无足够改善
- Core 变异次数达到 `max_mutations`（默认 8）
- 无活跃 Core 剩余
- 会话达到 `session_max_rounds`（默认 20）

---

## 8. 许可证与免责声明

本项目仅用于量化研究与流程自动化示例。请严格遵守 WorldQuant Brain 平台规则与账户条款，控制请求频率与并发，避免对服务造成不必要压力。
