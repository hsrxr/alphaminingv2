# AlphaMiningV2 因子搜索架构诊断与改进方案

作者：Manus AI

针对昨日在 WorldQuant Brain 平台上进行 `TPL_GROUP_IVHV_SMOOTH_V1` 模板回测时“一千多个因子里一个能用的都没有”的现象，我们对 AlphaMiningV2 的生成与回测架构进行了深度分析。分析表明，当前系统并非单纯因为“因子数量堆积”而失效，而是由于底层**搜索空间爆炸、语义分配错误以及缺乏自适应搜索策略**共同导致的无效计算。

以下是详细的诊断报告及系统性改进方案。

## 1. 架构问题诊断

### 1.1 灾难性的语义错配：100% 因子生成错误
在对昨日 `option8_TPL_GROUP_IVHV_SMOOTH_V1` 批次中 5000 个因子进行拆解后，我们发现了一个致命的 Bug：**所有因子的 IV 和 HV 字段分配都是反的或错误的**。

在 `template_catalog.json` 中，`iv_mean_field` 槽位的 `search_domain` 配置为：
```json
"include_regex": ["iv", "implied"],
"exclude_regex": ["hv", "hist", "realized"],
"fallback_to_all": true
```
然而，`historical_volatility_*` 字段既不包含 `iv/implied`，也不包含 `hv/hist/realized`（它包含的是 `historical`）。这导致 `include_regex` 匹配失败，触发了 `fallback_to_all=true`。最终，系统将 `historical_volatility_*` 强行塞入了 `iv_mean_field` 槽位。同理，`implied_volatility_*` 也因为未命中 `hv_field` 的 `include_regex` 而通过 fallback 被错误分配。

这导致昨日回测的 5000 个因子全部形如 `subtract(historical_volatility_10, implied_volatility_call_10)` 或 `subtract(historical_volatility_10, historical_volatility_120)`，完全违背了 IV 减去 HV 的金融逻辑，因此全军覆没是必然结果。

### 1.2 暴力的笛卡尔积搜索导致维度灾难
抛开上述 Bug 不谈，当前的生成逻辑（`main.py` 中的 `iter_template_expressions` 和 `iter_settings`）完全依赖暴力的笛卡尔积展开。以 `TPL_GROUP_IVHV_SMOOTH_V1` 为例：
- `iv_mean_field` (约 30+ 候选) × `hv_field` (约 30+ 候选)
- `smooth_days`: [3, 5, 10, 21, 63, 126, 252] (7 种)
- `group`: [market, sector, industry, subindustry] (4 种)
- `settings`: 默认展开 (如 decay, neutralization 等)

一个基础的 Core（即一对特定的 IV 和 HV 字段）会被无差别地放大 `1 × 1 × 7 × 4 = 28` 倍。这意味着系统会为每一个 Core 提交 28 次回测，其中包含大量高度相关的参数组合（如 `smooth_days=3` 和 `smooth_days=5` 的相关性极高）。这种架构缺乏“代表性探测”，把宝贵的并发配额浪费在了微调参数上。

### 1.3 因子生成与回测的“开环”架构
目前的流水线是开环的（Open-loop）：
1. `main.py` 一次性生成数万个因子落盘。
2. `backtest_runner.py` 盲目地、机械地消费这些 JSON 文件。
3. `result_filter.py` 仅用于事后筛选。

系统没有任何机制能在回测过程中发现“某个 Core 的前几个代表性参数表现极差”并据此提前终止（Early Stopping）该 Core 的后续测试。这印证了您的观察：有些 Core 只要测几个代表性参数就知道不行，但系统依然会机械地遍历完它的所有组合。

---

## 2. 系统性改进方案

为了解决上述问题，我们需要将 AlphaMiningV2 从“盲目的暴力生成器”升级为“智能的自适应搜索代理”。

### 2.1 修复数据域过滤逻辑 (Search Domain)
**问题核心**：`fallback_to_all=true` 是极其危险的默认行为，它掩盖了正则表达式匹配失败的问题。

**改进动作**：
1. 在 `template_catalog.json` 中，将所有数据域过滤的 `fallback_to_all` 设置为 `false`。
2. 修正正则表达式，使其准确匹配数据集中的实际字段名。例如：
   - IV 槽位：`"include_regex": ["implied_volatility"]`
   - HV 槽位：`"include_regex": ["historical_volatility", "parkinson_volatility"]`
3. 在 `main.py` 的 `apply_dataset_field_domain` 函数中加入断言：如果过滤后候选列表为空，应抛出异常或跳过该模板，而不是静默回退到全量字段。

### 2.2 引入“参数重要性分层” (Parameter Stratification)
您的直觉非常准确：对于不同类型的因子，参数的敏感度是不同的。我们应该优先搜索敏感参数，对不敏感参数进行抽样。

**改进动作**：
在 `template_catalog.json` 的 `slots` 定义中引入 `search_priority` 或 `representative_values` 字段。
例如，对于平滑时间，不应无脑遍历 7 个值，而是定义代表性子集：
```json
"smooth_days": {
  "values": [3, 5, 10, 21, 63, 126, 252],
  "representative_values": [5, 21, 126]
}
```
生成器在第一阶段（探测期）只使用 `representative_values` 生成因子。这样单个 Core 的测试量可以从 28 次降低到 12 次（3 × 4），节省 57% 的算力。

### 2.3 升级为闭环的“探测-展开”架构 (Probe & Expand)
这是架构重构的核心。我们需要打破生成与回测的物理隔离，建立反馈循环。

**实施路径**：
1. **定义 Core 签名**：在生成因子时，提取其逻辑核心（如 `subtract(implied_volatility_call_10, historical_volatility_10)`）作为 `core_id`，写入 JSON 批次中。
2. **两阶段生成**：
   - **Phase 1 (Probe)**：`main.py` 仅针对每个 Core 生成少数几个代表性参数（如 `nanHandling=OFF`, `smooth_days=21`, `group=industry`）的因子，形成探测批次（Probe Batch）。
   - **Phase 2 (Expand)**：开发一个新的中间调度脚本 `adaptive_scheduler.py`。它监控探测批次的回测结果，计算该 Core 的平均 Sharpe 或 Fitness。如果探测结果达标（如 Sharpe > 0.8），则触发生成该 Core 的全量参数网格（Expand Batch）；如果探测失败，则直接抛弃该 Core，转向下一个。
3. **动态 Settings 搜索**：将 `nanHandling`、`neutralization` 等全局设置纳入相同的探测逻辑。例如，资本面因子在探测期分别测试 `nanHandling=ON` 和 `OFF`，根据结果决定后续扩展方向。

### 2.4 引入算子变异与组合机制 (Mutation & Composition)
当一个因子的代表性参数表现一般时，系统应该具备“换算符”或“组合因子”的能力。

**改进动作**：
1. 在 `common_operator_slot_mappings.json` 中定义算子的相似度矩阵。如果 `ts_mean` 失败，不要去试 `ts_decay_linear`，而是尝试引入非线性的 `ts_rank`。
2. 开发因子组合模板（如 `TPL_COMBO_WEIGHTED_V1`），当发现两个不同 Core 的单因子表现尚可但达不到极优时，自动将它们作为子组件填入组合模板中进行二次回测。

---

## 3. 结论

您对因子的反思完全击中了当前架构的痛点。AlphaMiningV2 目前的低效，表面上是因为生成了大量无用的参数组合，本质上是因为系统缺乏对“因子核心（Core）”的认知和反馈机制，同时被隐藏的正则过滤 Bug 扭曲了搜索空间。

建议优先修复 `fallback_to_all` 导致的语义错误，随后着手引入“探测-展开（Probe & Expand）”的两阶段调度机制。这将使因子挖掘的效率和质量产生质的飞跃。
