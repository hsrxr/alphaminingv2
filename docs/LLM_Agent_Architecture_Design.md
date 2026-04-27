# 在 AlphaMiningV2 中引入 LLM Agent 的架构设计与可行性分析

作者：Manus AI

针对您提出的“是否建议在当前架构中加入 LLM 形成一个 Agent，完全自动化地完成因子的生成、回测、筛选、改进”的问题，我们的核心结论是：**强烈建议引入 LLM Agent，但绝不能让 LLM 接管所有流程，而是应采用“LLM 作为大脑 + 代码规则作为四肢”的混合架构（Hybrid Agent Architecture）。**

以下是详细的可行性分析、价值风险评估以及具体的架构设计建议。

## 1. 为什么需要 LLM Agent？

在量化因子的搜索中，最大的痛点不是“如何生成代码”，而是“**如何在无限的搜索空间中找到有逻辑支撑的方向**”。

当前的 `main.py` 只是一个机械的枚举器。它不知道 `historical_volatility` 和 `implied_volatility` 在金融逻辑上的对立关系，也不知道 `smooth_days=3` 和 `5` 的信息重叠度。而这正是 LLM 最擅长的地方：**理解变量的语义、推导金融逻辑、并根据反馈调整假设**。

![LLM 价值与风险矩阵](llm_value_risk_matrix.png)

如上图所示，我们对因子挖掘流水线的各个阶段进行了 LLM 价值与风险的量化评估：

*   **高价值/低风险区（强烈建议 LLM 介入）**：
    *   **模板设计（Template Design）**：LLM 可以根据研报或金融直觉，设计出逻辑自洽的表达式骨架。
    *   **算子变异（Mutation）**：当 `ts_mean` 失败时，LLM 知道应该尝试非线性的 `ts_rank` 或 `ts_zscore`，而不是盲目遍历所有算子。
    *   **结果解释（Result Interpretation）**：LLM 能够综合 Sharpe、Turnover 和 Fitness，判断一个 Core 是“毫无希望”还是“值得抢救”。
*   **高风险/低价值区（坚决交由代码规则处理）**：
    *   **并发调度与提交（Submission Scheduling）**：Brain 平台有严格的并发限制（最大 3 个 worker）和 HTTP 状态码约束。让 LLM 直接调用 API 提交回测会导致严重的 Rate Limit 错误和资源浪费。
    *   **字段过滤（Field Selection）**：LLM 容易产生幻觉，凭空捏造不存在的字段名。这部分必须依赖严格的正则表达式和 API 数据字典。

## 2. 混合架构设计 (Hybrid Agent Architecture)

基于上述分析，我们建议的架构并非让 LLM 独立写一个 Python 脚本去跑，而是将 LLM 嵌入到现有的流水线中，形成一个**闭环反馈系统（Closed-Loop System）**。

![流程对比图](pipeline_comparison.png)

### 2.1 架构组件分工

1.  **LLM Planner（策略大脑）**
    *   **职责**：负责提出假设（Hypothesis）。例如：“我想测试期权隐含波动率偏度（IV Skew）对未来收益的预测能力”。
    *   **输出**：生成一个标准化的 `Template JSON`（包含 Core 的定义和需要探测的代表性参数）。
2.  **Rule-based Generator（规则生成器 - 现有 `main.py` 的升级版）**
    *   **职责**：将 LLM 的意图安全地落地。它会严格校验 LLM 提供的字段是否存在于 `datafields_cache` 中，并生成**探测批次（Probe Batch）**。
    *   **改进**：只生成代表性参数（如 `smooth_days=[5, 21]`），拒绝 LLM 提出的全量遍历请求。
3.  **Backtest Runner（执行引擎 - 现有 `backtest_runner.py`）**
    *   **职责**：保持不变，负责稳健地并发提交、重试、处理网络异常。
4.  **LLM Evaluator（评估与进化引擎）**
    *   **职责**：读取 `backtest_results`，对探测结果进行归因分析。
    *   **决策输出**：
        *   *Expand（展开）*：如果 Sharpe > 1.0，指令 Generator 生成该 Core 的全量参数网格。
        *   *Mutate（变异）*：如果 Sharpe 在 0.5-1.0 之间，指令 Planner 修改算子（如加个 `ts_decay_linear`）。
        *   *Abandon（抛弃）*：如果 Sharpe < 0，直接终止该 Core 的搜索。

### 2.2 效率提升预期

引入这种“探测-评估-展开”的 Agent 架构后，算力效率将得到指数级提升。

![效率对比图](efficiency_comparison.png)

如上图所示，在传统的暴力枚举中，测试 1000 个 Core 需要 28,000 次回测。而在 LLM Agent 架构中，由于探测期只测试 3-5 个代表性参数，且大量无效 Core 被 LLM Evaluator 提前抛弃，**总回测成本可降低 70% 以上**，同时产出的因子在逻辑上更具解释性。

## 3. 实施路径建议

如果您决定推进 LLM Agent 的接入，建议按照以下三个阶段逐步实施，避免过度设计导致系统崩溃：

### 阶段一：重构基础，实现“代码级”的 Probe-Expand（1-2周）
在引入 LLM 之前，必须先让现有的 Python 架构具备闭环能力。
1.  修复 `template_catalog.json` 中导致语义错误的正则 Bug。
2.  在模板中引入 `representative_values` 字段。
3.  开发 `adaptive_scheduler.py`，实现基于代码规则的“探测-展开”逻辑（例如：平均 Sharpe > 0.8 则展开）。

### 阶段二：引入 LLM Evaluator 作为“副驾驶”（Copilot）（2-3周）
在调度器中接入 LLM API（如 OpenAI 或 Anthropic）。
1.  将阶段一中表现平庸（Sharpe 0.5-0.8）的因子结果，连同其表达式喂给 LLM。
2.  让 LLM 输出“改进建议”（Mutate Suggestions），例如替换算子或修改中性化（Neutralization）方式。
3.  将 LLM 的建议解析回 JSON 批次，重新投入回测。

### 阶段三：引入 LLM Planner 实现完全自治（Auto-Pilot）（长期）
1.  赋予 LLM 读取 `datafields_store.py` 缓存的能力。
2.  让 LLM 每天根据最新的金融市场动态或特定的因子研报，自主生成全新的 `Template JSON`。
3.  系统实现 24/7 不间断的“假设生成 → 探测 → 评估 → 进化”循环。

## 4. 核心风险提示

*   **幻觉风险（Hallucination）**：LLM 极易编造 Brain 平台上不存在的算子（如 `ts_ewma`）或数据字段。**对策**：必须在 LLM 和 Backtest Runner 之间设置严格的语法校验层（Parser & Validator）。
*   **配额消耗（Quota Burn）**：如果 LLM 的评估逻辑过于宽容，可能会导致大量无意义的 Expand 操作，耗尽 WorldQuant Brain 的每日提交配额。**对策**：在 `adaptive_scheduler.py` 中设置硬性的每日回测上限（如 2000 次/天）。

## 总结

将 LLM 引入 AlphaMiningV2 是极具潜力的方向。它能将系统从**“算力密集型”转化为“逻辑密集型”**。只要坚持“LLM 负责逻辑推导，代码负责执行与约束”的边界，就能打造出一个高效、智能的自动化因子挖掘流水线。
