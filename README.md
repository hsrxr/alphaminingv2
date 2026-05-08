# AlphaMiningV2

面向 WorldQuant Brain 平台的**全自主 Alpha 因子挖掘系统**。核心是一个 LLM 驱动的直接探索型 Agent：它像人类量化研究员一样，通过工具调用探索数据集、研究算子、构造 FASTEXPR 表达式、提交回测、分析结果并持续迭代——全程无需预定义模板。

```
Research → Submit 3 factors → Poll → Analyze → Improve → ... → Converge
         ↕ tools (14个)
  Datasets · Fields · Operators · Settings · Knowledge Base
```

---

## 快速开始

```bash
# 从投资直觉出发，让 Agent 自主选择数据集
python -m agent.direct_agent \
  --idea "Momentum combined with low volatility"

# 指定数据集（跳过探索步骤，更快）
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

这是系统的核心。与传统模板枚举式因子挖掘不同，Direct Agent 让 LLM 通过 14 个工具直接与 Brain 平台交互：

1. **Research** — LLM 调用工具探索数据集字段、算子功能、回测设置
2. **Submit** — LLM 构造 FASTEXPR 表达式，Agent 校验后提交 Brain 回测（最多 3 并发）
3. **Iterate** — 每 30 秒轮询，因子完成即触发 LLM 分析，生成改进版本替换之。暂时性网络错误自动进入重试队列，后台持续轮询直至恢复或确认失败
4. **Converge** — 采用分级质量标准：Sharpe>1.25 / Fitness>1.0 是**最低及格线**，达到 Sharpe>1.5 且换手率<0.30 才视为优质收敛。LLM 自行判断因子是否还有改进空间，避免止步于"刚及格"

详细协议、工具列表和运行原理见 [docs/DIRECT_AGENT.md](./docs/DIRECT_AGENT.md)。

### Pipeline — 回测基础设施

提供 Brain API 并发回测引擎（3 并发、指数退避重试、checkpoint 续跑）、数据字段缓存等底层能力。Agent 的所有回测请求最终由此执行。

Agent 层内置可靠性机制：
- **透明重试** — 暂时性网络错误（代理超时、连接重置、5xx）自动重试 3 次后转入后台重试队列持续轮询，不丢失回测结果
- **网络错误隔离** — 网络错误与因子本身失败区分处理，避免因临时故障误判因子无效

### 知识库

Agent 每次迭代将可复用的经验写入 `agent_output/knowledge_base.json`，后续会话自动搜索相关条目加载到上下文。系统强制限制每轮最多保存 2 条、每会话最多 10 条，防止知识库膨胀稀释有效信息。

---

## 近期改进

### v2.1 — 轮询可靠性与收敛质量分级 (2026-05)

- **重试队列** — 暂时性网络错误（代理超时、连接重置、5xx）自动重试 3 次后转入后台队列持续轮询，恢复后自动重新注入分析流程；真实因子失败仍立即标记
- **收敛分级** — Sharpe>1.25 / Fitness>1.0 定义为最低及格线而非收敛目标；引入质量评估表（Converge-worthy / Keep improving / Abandon），LLM 自行判断改进价值
- **顺序修复** — 收敛检查移至 improvement 提交之前，避免新提交的 running job 阻塞会话退出
- **知识库硬限制** — 每轮最多保存 2 条、每会话最多 10 条，防止知识库膨胀
- **LLM 异常兜底** — 捕获 DeepSeek API 网络异常（ChunkedEncodingError 等），避免偶发网络故障导致会话崩溃
- **poll 缓存修复** — `poll_results` 不再缓存暂时性网络错误，确保重试队列能真正重新查询 API

---

## 更多文档


| 文档                                         | 内容                                      |
| -------------------------------------------- | ----------------------------------------- |
| [docs/DIRECT_AGENT.md](docs/DIRECT_AGENT.md) | Direct Agent 完整架构、工具协议、运行模式 |
| [docs/PIPELINE.md](docs/PIPELINE.md)         | 回测流水线（独立于 Agent 使用）           |
| [docs/BRAIN_API_REFERENCE.md](docs/BRAIN_API_REFERENCE.md) | Brain 回测 API 传参与返回参数参考 |
