# AlphaMiningV2

面向 WorldQuant Brain 平台的 **Alpha 因子挖掘系统**。提供两套运行模式：

- **DirectAgent** — 单 Agent 直接探索模式，LLM 通过工具调用自主完成研究→构造→提交→分析→改进的完整循环
- **MultiAgent** — 多 Agent 协作模式，将探索（Explorer）和优化（Optimizer）分离为独立 Agent 系统化搜索因子空间

```
                                   ┌─────────────────────────────┐
                                   │     IdeaScout (可选)         │
                                   │  自动发现因子想法（网络搜索）  │
                                   └──────────┬──────────────────┘
                                              │ 投资假设
                    ┌─────────────────────────┴─────────────────────────┐
                    │                   OrchestratorV2                   │
                    │   ┌──────────────┐         ┌──────────────┐      │
                    │   │   Explorer    │ ──────→ │  Optimizer   │      │
                    │   │  LLM 探索新结构│ 候选因子 │  参数调优    │      │
                    │   └──────────────┘         └──────────────┘      │
                    └────────────────────┬─────────────────────────────┘
                                         │ 提交/轮询
                    ┌────────────────────┴─────────────────────────────┐
                    │               WQ Tools (16 tools)                │
                    │      Brain API 工具层：认证 · 提交 · 轮询 · 分析    │
                    └──────────────────────────────────────────────────┘
```

---

## 快速开始

```bash
# ── DirectAgent（单 Agent） ──
python -m agent.direct_agent --idea "Momentum combined with low volatility"

# ── MultiAgent（多 Agent 协作） ──
python -m agent.multi_agent --idea "Companies with high FCF outperform"

# 从已有表达式开始（跳过探索，直接优化参数）
python -m agent.multi_agent --expression "ts_decay_linear(ts_scale(est_cashflow_op,252),22)"

# 自动发现因子想法（无需 --idea）
python -m agent.multi_agent --auto-discover
```

需要配置凭证，参见[环境配置](#环境配置)。

---

## 目录结构

```
alphaminingv2/
├── agent/                           # Agent 核心
│   ├── multi_agent.py              #   多 Agent 入口
│   ├── direct_agent.py             #   单 Agent 入口
│   ├── orchestrator_v2.py          #   多 Agent 编排器
│   ├── explorer.py                 #   LLM 表达式探索器
│   ├── param_optimizer.py          #   参数优化器（脚本/LLM 双模式）
│   ├── idea_scout.py               #   因子想法自动发现
│   ├── expression_fingerprint.py   #   表达式结构去重
│   ├── results_store.py            #   结果共享数据库
│   ├── config.py                   #   多 Agent 配置中心
│   │
│   ├── wq_tools.py                 #   WQ Brain 工具层（16 个工具）
│   ├── llm_client.py               #   DeepSeek API 客户端
│   ├── llm_logger.py               #   LLM 调用日志
│   ├── expression_validator.py     #   FASTEXPR 语法校验
│   ├── mutation_engine.py          #   因子变异引擎
│   ├── result_analyzer.py          #   回测结果 LLM 分析
│   ├── template_generator.py       #   金融直觉 → 模板（备选）
│   ├── orchestrator.py             #   模板式 Agent（备选）
│   ├── convergence.py              #   收敛检测
│   ├── memory.py                   #   会话持久化
│   └── feedback_loop.py            #   交互式循环
│
├── pipeline/                        # 回测流水线（底层基础设施）
│   ├── backtest_runner.py          #   并发回测执行引擎
│   ├── datafields_store.py         #   数据字段缓存
│   ├── result_filter.py            #   结果筛选与聚合
│   ├── adaptive_scheduler.py       #   Probe-Expand 调度器
│   └── run_pipeline.py             #   一键入口
│
├── wq_operators_cleaned.json       # WQ 算子定义
├── .env.example                    # 环境变量模板
│
├── agent_output/                   # Agent 会话输出（gitignored）
├── datafields_cache/               # 数据字段缓存（gitignored）
└── backtest_results/               # 回测结果（gitignored）
```

---

## 环境配置

### 凭证

| 变量               | 说明                    | 用途      |
| ------------------ | ----------------------- | --------- |
| `BRAIN_USERNAME`   | WorldQuant Brain 用户名 | 回测提交  |
| `BRAIN_PASSWORD`   | WorldQuant Brain 密码   | 回测提交  |
| `DEEPSEEK_API_KEY` | DeepSeek API 密钥       | Agent LLM |

放在项目根目录 `.env` 文件或进程环境变量中。

### 数据集

使用 `datafields_store.py` 缓存目标数据集的字段定义：

```bash
python pipeline/datafields_store.py --dataset-id pv1
```

---

## 核心概念

### DirectAgent — 单 Agent 直接探索

LLM 通过 14 个工具与 Brain 平台交互，在事件驱动循环中自主完成因子挖掘：

1. **Research** — 探索数据集字段、算子功能、回测设置
2. **Submit** — 构造 FASTEXPR 表达式，校验后提交回测（最多 3 并发）
3. **Iterate** — 每 30 秒轮询，因子完成即触发 LLM 分析，生成改进版本替换之
4. **Converge** — 分级质量标准（Sharpe≥1.25 / Fitness≥1.0 为及格线），LLM 自行判断改进空间

DirectAgent 适合需要灵活探索的单次会话，LLM 完全控制研究方向和改进策略。

### MultiAgent — 多 Agent 协作

将因子挖掘分解为两个专业化阶段，由 OrchestratorV2 协调：

| 阶段 | Agent | 职责 |
|------|-------|------|
| **探索** | Explorer | 基于投资假设和已测结果，生成**结构不同**的候选表达式。使用 `expression_fingerprint` 避免重复结构 |
| **优化** | ParamOptimizer | 对候选因子进行系统性参数调优。支持脚本模式（确定性网格搜索）和 LLM 模式（基于历史结果智能推荐） |

工作流程：

```
Idea → Explorer 生成 3 个候选 → 默认设置提交 → Sharpe≥0.8? → 是 → Optimizer 调优参数
                                                                    ↓
                                                             达到目标 Sharpe? → 收敛 / 继续
```

IdeaScout 模块可在无 `--idea` 时通过网络搜索自动发现因子想法。

### 回测基础设施

Pipeline 提供 3 并发回测引擎，支持指数退避重试、checkpoint 续跑。Agent 层在之上增加了透明重试队列和网络错误隔离机制。

### 知识库

DirectAgent 将可复用的经验写入 `knowledge_base.json`，后续会话自动加载相关条目到上下文。每轮最多保存 2 条、每会话最多 10 条。

---

## 近期改进

### v2.2 — 多 Agent 架构 (2026-05)

- **多 Agent 编排** — OrchestratorV2 协调 Explorer 和 Optimizer 两个专业化 Agent
- **Explorer** — LLM 驱动的结构探索，使用 expression fingerprint 避免重复
- **ParamOptimizer** — 支持确定性脚本搜索和 LLM 智能推荐两种参数调优模式
- **IdeaScout** — Web 搜索驱动的因子想法自动发现模块
- **ResultsStore** — 线程安全的结果共享数据库，Agent 间数据互通
- **Config 中心化** — `AgentConfig` 统一管理所有可配参数

### v2.1 — 轮询可靠性与收敛质量分级 (2026-05)

- 透明重试队列 / 收敛分级 / 网络错误隔离 / 知识库硬限制 / LLM 异常兜底

---

## 更多文档

| 文档 | 内容 |
|------|------|
| 项目根目录 [docs/](./docs/) | DirectAgent 架构、回测流水线等详细文档 |
