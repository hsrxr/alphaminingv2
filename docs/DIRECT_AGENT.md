# Direct Agent 架构与使用说明

## 概述

Direct Agent 是 AlphaMiningV2 的核心组件。它让 LLM 像人类量化研究员一样工作：通过工具调用探索数据集字段、研究算子语义、理解回测设置，然后直接构造 FASTEXPR 表达式并提交回测。整个过程不需要任何预定义模板。

### 与模板式 Agent 的区别


| 维度     | 模板式 Agent (orchestrator)      | Direct Agent (direct_agent)   |
| -------- | -------------------------------- | ----------------------------- |
| 模板     | 需要 LLM 先生成模板，再枚举填充  | 不需要模板，LLM 直接写表达式  |
| 搜索空间 | 由模板槽位决定，受笛卡尔积限制   | 由 LLM 创造力决定，无枚举上限 |
| 并发策略 | Probe → Expand 两阶段           | 固定 3 并发，事件驱动替换     |
| 迭代粒度 | 批量生成 → 批量回测 → 批量分析 | 单个因子完成即触发分析        |
| 知识利用 | 无持久化                         | 文件知识库，跨会话复用        |

---

## 架构

### 三阶段协议

```
┌─────────────────────────────────────────────────────────────────┐
│  Phase 1 — Research                                              │
│  LLM 调用工具探索，直到发出 type: "submit"                        │
│                                                                   │
│  list_datasets → list_fields → search_operators → get_setting_... │
│  validate_expression → search_knowledge → ... → "submit"         │
└──────────────────────────────┬──────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│  Phase 2 — Iteration                                            │
│  3 个并发回测，事件驱动分析迭代                                   │
│                                                                   │
│  Submit 3 factors ─→ Brain API ─→ Poll (30s) ─→ Any done?       │
│                                                     ↓            │
│  LLM analyzes results ─→ improvements ─→ resubmit ─→ poll        │
│       ↑_____________________________________________↓            │
│                                          直到 converged 或耗尽轮次 │
└──────────────────────────────┬──────────────────────────────────┘
                               ↓
┌─────────────────────────────────────────────────────────────────┐
│  Phase 3 — Report                                               │
│  汇总结果、记录最佳因子到知识库、写 report.json                    │
└─────────────────────────────────────────────────────────────────┘
```

### 工具层（16 个工具）

Agent 通过以下工具与 WorldQuant Brain 交互。工具分为四类：

#### 数据探索


| 工具                  | 参数                 | 返回                                     |
| --------------------- | -------------------- | ---------------------------------------- |
| `list_datasets`       | —                   | 所有本地缓存数据集及其字段数             |
| `list_fields`         | dataset_id           | 数据集中所有字段的 id + description      |
| `get_field_detail`    | field_id, dataset_id | 字段完整元数据（类别、覆盖度、用户数等） |
| `list_all_operators`  | —                   | 全部 50 个 WQ 算子名称、语法、简要说明   |
| `search_operators`    | keyword              | 按关键词搜索算子（名称、说明、详细解释） |
| `get_operator_detail` | name                 | 算子的完整定义、语法、示例               |

#### 回测设置


| 工具                 | 参数 | 返回                                     |
| -------------------- | ---- | ---------------------------------------- |
| `get_setting_schema` | —   | 全部 10 个模拟参数的定义、可选值、默认值 |
| `get_setting_detail` | name | 单个参数的完整说明                       |
| `get_settings_guide` | —   | 抓取 Brain 官方设置文档页面              |

#### 表达式校验


| 工具                  | 参数                   | 返回                                   |
| --------------------- | ---------------------- | -------------------------------------- |
| `validate_expression` | expression, dataset_id | 括号平衡、算子名存在性、字段存在性检查 |

#### 知识库


| 工具                    | 参数                   | 返回                   |
| ----------------------- | ---------------------- | ---------------------- |
| `search_knowledge`      | keyword                | 按关键词搜索知识库条目 |
| `add_knowledge`         | topic, insight, source | 添加经验条目到知识库   |
| `list_knowledge_topics` | —                     | 所有知识库主题及条目数 |

### 响应协议

LLM 通过 JSON 格式回复驱动 Agent 执行。Agent 解析 `type` 字段并调度：

#### tool_call — 调用工具

```json
{
  "type": "tool_call",
  "reasoning": "I need to understand what fields are available in pv13...",
  "tool": "list_fields",
  "args": {
    "dataset_id": "pv13"
  }
}
```

Agent 执行工具，将结果以 user 消息追加回对话。

#### submit — 提交因子

```json
{
  "type": "submit",
  "reasoning": "Based on my research, I'll construct 3 momentum factors...",
  "expressions": [
    {
      "expression": "group_rank(ts_mean(returns, 21), industry)",
      "settings": {
        "neutralization": "INDUSTRY",
        "decay": 5
      },
      "rationale": "21-day momentum, industry neutralized to reduce sector bias"
    }
  ]
}
```

Agent 逐条校验，通过后提交 Brain API，并在 `active_jobs` 中跟踪。

#### analyze — 分析结果并改进

```json
{
  "type": "analyze",
  "reasoning": "Factor 1 has sharpe 1.2 and moderate turnover...",
  "converged": false,
  "improvements": [
    {
      "replace_job_id": "https://api.worldquantbrain.com/simulations/abc123",
      "expression": "group_rank(ts_mean(returns, 63), industry)",
      "settings": {
        "neutralization": "INDUSTRY",
        "decay": 15
      },
      "rationale": "Longer lookback and higher decay to reduce turnover"
    }
  ],
  "knowledge": [
    {
      "topic": "turnover",
      "insight": "21-day momentum with INDUSTRY neutralization achieved sharpe ~1.2 but turnover 0.8. Longer lookback can reduce turnover."
    }
  ]
}
```

Agent 保存知识条目、标记被替换的 job、校验并提交改进表达式。

#### done — 结束会话

```json
{
  "type": "done",
  "reasoning": "Best sharpe 1.5 achieved. No further improvements likely.",
  "summary": {
    "best_sharpe": 1.5,
    "total_submissions": 9,
    "key_insights": ["Momentum works best with INDUSTRY neutralization"]
  }
}
```

---

## 运行模式

### 模式 1：从投资直觉出发（推荐）

不指定数据集，让 Agent 自主探索：

```bash
python -m agent.direct_agent \
  --idea "Momentum combined with low volatility"
```

Agent 会先调用 `list_datasets()` 查看可用数据集，然后选择最相关的进行探索。

也可以指定数据集加速：

```bash
python -m agent.direct_agent \
  --dataset-id pv13 \
  --idea "Momentum combined with low volatility" \
  --iterations 15 \
  --target-sharpe 2.0
```

LLM 会先研究 pv13 数据集有哪些字段，找到 `returns`、`volume` 等相关字段，然后构造动量因子、低波因子及其组合。

### 模式 2：改进已有表达式

```bash
python -m agent.direct_agent \
  --expression "group_rank(ts_mean(returns,21),industry)" \
  --critique "Sharpe 0.8, turnover too high at 0.9"
```

LLM 分析当前表达式的不足，生成改进变体。

### 模式 3：纯改进，无特定批评

```bash
python -m agent.direct_agent \
    --expression "ts_mean(returns,5) - ts_mean(returns,21)"
```

LLM 会分析该表达式可能的弱点并尝试改进方向。

---

## 知识库

知识库存储在 `agent_output/knowledge_base.json`，是一个持久化的经验记录文件。

### 自动记录

Agent 会在以下情况自动写入知识库：

- **分析阶段** — LLM 在 analyze 响应中带 `knowledge` 字段
- **会话结束** — 最佳因子 Sharpe > 1.0 时自动记录

### 跨会话利用

新会话启动时，Agent 会自动：

1. 按 `dataset_id` 搜索知识库
2. 按 idea/expression 中的关键词搜索
3. 将相关条目附加到初始 prompt

### 示例条目

```json
{
  "id": 7,
  "topic": "turnover",
  "insight": "Longer lookback (63d vs 21d) reduces turnover from 0.9 to 0.4 with minimal sharpe loss",
  "source": "agent",
  "timestamp": "2026-05-07T09:30:00"
}
```

---

## 关键参数


| 参数              | 默认值        | 说明                 |
| ----------------- | ------------- | -------------------- |
| `--iterations`    | 20            | 最大分析迭代轮次     |
| `--target-sharpe` | 2.0           | 达到此 Sharpe 即收敛 |
| `--model`         | deepseek-chat | LLM 模型             |
| `--output-dir`    | agent_output  | 输出目录             |
| `--quiet`         | false         | 静默模式             |

---

## 输出结构

```
agent_output/
├── direct_20260507_093000/
│   └── report.json              # 完整会话报告（含所有因子指标）
│
└── knowledge_base.json          # 持久化知识库（跨会话共享）
```

### report.json 格式

```json
{
  "session_id": "20260507_093000",
  "dataset_id": "pv13",
  "idea": "Momentum combined with low volatility",
  "iterations": 5,
  "converged": true,
  "total_submissions": 12,
  "results": [
    {
      "expression": "group_rank(ts_mean(returns,63),industry)",
      "status": "completed",
      "sharpe": 1.5,
      "turnover": 0.4,
      "fitness": 0.8,
      "alpha_id": "alpha_xxxxx",
      "round": 3
    }
  ]
}
```

结果按 Sharpe 降序排列。
