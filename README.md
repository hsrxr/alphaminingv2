# AlphaMiningV2

面向 WorldQuant Brain 平台的 **自主 Alpha 因子挖掘系统**。核心是 LLM 驱动的 Direct Agent：像人类量化研究员一样，通过工具调用探索数据集、研究算子、构造 FASTEXPR 表达式、提交回测、分析结果并持续迭代。

```
Research → Submit → Poll → Analyze → Improve → ... → Converge
         ↕ tools (15个)
  Datasets · Fields · Operators · Settings · Knowledge Base
```

---

## 快速开始

```bash
# 从投资直觉出发
python -m agent.direct_agent \
  --idea "Momentum combined with low volatility"

# 指定数据集，跳过探索步骤
python -m agent.direct_agent \
  --dataset-id pv13 \
  --idea "Momentum combined with low volatility"

# 改进已有表达式
python -m agent.direct_agent \
  --dataset-id pv13 \
  --expression "group_rank(ts_mean(returns,21),industry)" \
  --critique "High turnover, try longer lookback"
```

需要配置凭证，参见[环境配置](#环境配置)。

---

## 目录结构

```
alphaminingv2/
├── agent/                           # Agent 核心
│   ├── direct_agent.py             #   DirectAgent：主入口
│   ├── wq_tools.py                 #   WQ Brain 工具层（16 个工具）
│   ├── llm_client.py               #   DeepSeek API 客户端
│   ├── llm_logger.py               #   LLM 调用日志
│   ├── expression_validator.py     #   FASTEXPR 语法校验
│   ├── mutation_engine.py          #   因子变异引擎
│   ├── result_analyzer.py          #   回测结果 LLM 分析
│   ├── convergence.py              #   收敛检测
│   ├── memory.py                   #   会话持久化
│   ├── feedback_loop.py            #   交互式循环
│   ├── template_generator.py       #   金融直觉 → 模板（备选）
│   ├── orchestrator.py             #   模板式 Agent（备选）
│   │
│   ├── multi_agent.py              #   多 Agent 入口（实验性）
│   ├── orchestrator_v2.py          #   多 Agent 编排器
│   ├── explorer.py                 #   LLM 表达式探索器
│   ├── param_optimizer.py          #   参数优化器
│   ├── idea_scout.py               #   因子想法自动发现
│   ├── expression_fingerprint.py   #   表达式结构去重
│   ├── results_store.py            #   结果共享数据库
│   └── config.py                   #   多 Agent 配置
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

### DirectAgent — 事件驱动因子挖掘循环

LLM 通过 15 个工具与 Brain 平台交互，在一个事件驱动循环中完成从研究到收敛的全流程：

```
                    ┌─────────┐
                    │  Research│
                    │  探索阶段 │
                    └────┬────┘
                         ↓ 投资假设
              ┌──────────────────┐
              │  Iteration 循环   │
              │                  │
              │  ┌────────────┐  │
              │  │  Creative   │  │  ← 生成新方向（多方向探索）
              │  │   Phase     │  │
              │  └──────┬─────┘  │
              │         ↓        │
              │  ┌────────────┐  │
              │  │ Improvement│  │  ← 深耕现有方向（参数调优）
              │  │   Phase    │  │     进步停滞 → 切回 Creative
              │  └────────────┘  │
              │         ↓        │
              │   Converge?      │──→ 未达标 → 继续
              └────────┬─────────┘
                       ↓ 达标
                  ┌──────────┐
                  │   Stop   │
                  │  收敛退出  │
                  └──────────┘
```

#### Phase 状态机

迭代阶段分为两种**互斥状态**，由状态机自动切换：

| Phase | 行为 | 进入条件 | 切出条件 |
|-------|------|----------|----------|
| **Creative** | LLM 从当前结果出发，提出新方向（directions），每个方向附带初始表达式 | 会话开始 / Improvement 达到上限或停滞 | Creative 提交完成后自动切入 Improvement |
| **Improvement** | LLM 针对已有方向迭代改进表达式，不允许新建方向 | Creative 提交完成后进入 | 达到 8 轮上限 / 连续 5 轮未突破最佳 Sharpe / 所有方向被放弃 |

两种相位交替运行，构成完整的因子搜索策略：

- **Creative 阶段**防止 LLM 陷入局部最优——强迫定期跳出当前思路，提出新假设
- **Improvement 阶段**防止 LLM 浅尝辄止——强迫在既有方向上深入改进，积累有效迭代

每次状态切换都会重置相位计数器，确保 LLM 在新相位中有完整的轮次配额。

### 工作流程总览

1. **Research** — LLM 调用工具探索数据集字段、算子功能、回测设置，形成投资假设
2. **Creative** — LLM 提交多个方向（direction）及其初始表达式，提交后自动切入 Improvement
3. **Improvement** — LLM 在已有方向上迭代改进表达式，替换已完成回测的因子（保持 3 并发满负荷）
4. **Phase Switch** — 当 Improvement 达到轮次上限或进步停滞时，自动切回 Creative 生成新方向
5. **Converge** — LLM 判断因子是否达到质量标准（Sharpe / Fitness / Turnover），达到则终止会话

### 重试与可靠性

- 暂时性网络错误（代理超时、连接重置、5xx）自动重试 3 次后转入后台队列持续轮询，恢复后重新注入分析流程
- 因子本身失败（语法错误、校验不通过）立即标记，不浪费重试
- 网络错误与因子失败区分处理，避免因临时故障误判因子无效

### 知识库

每次迭代将可复用经验写入 `agent_output/knowledge_base.json`，后续会话自动搜索相关条目加载到上下文。每轮最多保存 2 条、每会话最多 10 条。

---

## 近期改进

### v2.2 — Creative / Improvement 相位分离 (2026-05)

- **状态机** — 迭代阶段拆分为 Creative（生成新方向）和 Improvement（深耕现有方向）两种互斥状态，自动切换
- **方向追踪** — 每个 direction 记录 improvement_count，LLM 必须链接到已有方向，防止无限新建无意义方向
- **停滞检测** — 连续 5 轮未突破最佳 Sharpe 触发相位切换，避免 LLM 在无效方向上原地打转
- **轮次上限** — Improvement 最多 8 轮后强制切回 Creative，确保定期探索新思路
- **多 Agent 框架（实验性）** — OrchestratorV2 + Explorer + ParamOptimizer 的解耦架构，`python -m agent.multi_agent`

### v2.1 — 轮询可靠性与收敛质量分级 (2026-05)

- **透明重试队列** — 暂时性网络错误自动重试 3 次后转入后台持续轮询，恢复后自动重新注入分析
- **收敛分级** — Sharpe≥1.25 / Fitness≥1.0 为最低及格线；引入质量评估，LLM 自行判断改进价值
- **网络错误隔离** — 网络错误与因子本身失败区分处理，避免临时故障误判因子无效
- **知识库硬限制** — 每轮最多 2 条、每会话最多 10 条
- **LLM 异常兜底** — 捕获 DeepSeek API 网络异常，避免偶发故障导致会话崩溃
