# AlphaMiningV2

面向 WorldQuant Brain 平台的**全自主 Alpha 因子挖掘系统**。核心是一个 LLM 驱动的直接探索型 Agent：它像人类量化研究员一样，通过工具调用探索数据集、研究算子、构造 FASTEXPR 表达式、提交回测、分析结果并持续迭代——全程无需预定义模板。

```
Research → Submit 3 factors → Poll → Analyze → Improve → ... → Converge
         ↕ tools (16个)
  Datasets · Fields · Operators · Settings · Knowledge Base
```

---

## 快速开始

```bash
# 从投资直觉出发，全自动挖掘因子
python -m agent.direct_agent \
  --dataset-id pv13 \
  --idea "Momentum combined with low volatility" \
  --iterations 10

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
│   ├── direct_agent.py             #   无模板 Direct Agent（主入口）
│   ├── wq_tools.py                 #   WQ Brain 工具层（16 个工具）
│   ├── llm_client.py               #   DeepSeek API 客户端
│   ├── expression_validator.py     #   WQ FASTEXPR 语法校验
│   ├── orchestrator.py             #   模板式 Agent（备选）
│   ├── template_generator.py       #   金融直觉 → 模板
│   ├── mutation_engine.py          #   因子变异引擎
│   ├── result_analyzer.py          #   回测结果 LLM 分析
│   ├── convergence.py              #   收敛检测
│   ├── memory.py                   #   会话持久化
│   └── feedback_loop.py            #   交互式循环
│
├── pipeline/                        # 回测流水线
│   ├── main.py                     #   因子生成器
│   ├── backtest_runner.py          #   并发回测执行引擎
│   ├── adaptive_scheduler.py       #   Probe-Expand 调度器
│   ├── result_filter.py            #   结果筛选与聚合
│   ├── datafields_store.py         #   数据字段缓存
│   └── run_pipeline.py             #   一键入口
│
├── adaptive_scheduler.py           # 根层封装（委托 pipeline/）
├── backtest_runner.py              # 保持向后兼容
├── datafields_store.py
├── main.py
├── result_filter.py
├── run_pipeline.py
│
├── template_catalog.json           # 预定义模板
├── wq_operators_cleaned.json       # 50 个 WQ 算子定义
├── .env.example                    # 环境变量模板
│
├── agent_output/                   # Agent 会话输出
│   ├── direct_20260507_093000/     #   Direct Agent 会话
│   │   └── report.json
│   └── knowledge_base.json         #   持久化知识库
│
├── datafields_cache/               # 数据字段缓存
├── backtest_results/               # 回测结果
└── docs/                           # 详细文档
    └── DIRECT_AGENT.md             #   Direct Agent 完整说明
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

本地缓存了以下数据集的字段定义（首次运行 Agent 前建议先缓存目标数据集）：


| 数据集          | 字段数 | 用途         |
| --------------- | ------ | ------------ |
| `pv1`           | 135    | 价格与成交量 |
| `option8`       | 64     | 期权 IV/HV   |
| `fundamental6`  | 574    | 基本面       |
| `analyst4`      | 469    | 分析师预期   |
| `news12`        | 75     | 新闻         |
| `sentiment1`    | 17     | 情绪         |
| `option9`       | —     | 期权 PCR     |
| `socialmedia12` | —     | 社交媒体     |

缓存数据字段：

```bash
python pipeline/datafields_store.py --dataset-id pv1
```

---

## 核心概念

### Direct Agent — 无模板直接探索

这是系统的核心。与传统模板枚举式因子挖掘不同，Direct Agent 让 LLM 通过 16 个工具直接与 Brain 平台交互：

1. **Research** — LLM 调用工具探索数据集字段、算子功能、回测设置
2. **Submit** — LLM 构造 3 条 FASTEXPR 表达式，Agent 校验后提交 Brain 回测
3. **Iterate** — 每 30 秒轮询，任一因子完成即触发 LLM 分析，生成改进版本替换之
4. **Converge** — 达到目标 Sharpe 或无法进一步改进时结束

详细协议、工具列表和运行原理见 [docs/DIRECT_AGENT.md](./docs/DIRECT_AGENT.md)。

### Pipeline — 回测基础设施

提供 Brain API 并发回测引擎（3 并发、指数退避重试、checkpoint 续跑）、数据字段缓存等底层能力。Agent 的所有回测请求最终由此执行。

### 知识库

Agent 每次迭代会将成功/失败的经验写入 `agent_output/knowledge_base.json`，后续会话自动加载相关条目，实现跨会话学习。

---

## 更多文档


| 文档                                         | 内容                                      |
| -------------------------------------------- | ----------------------------------------- |
| [docs/DIRECT_AGENT.md](docs/DIRECT_AGENT.md) | Direct Agent 完整架构、工具协议、运行模式 |
| [docs/PIPELINE.md](docs/PIPELINE.md)         | 回测流水线（独立于 Agent 使用）           |
