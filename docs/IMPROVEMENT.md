1. 模板是静态的、手动生成的 — 26 个模板限制了搜索空间，而且模板本身的表达式骨架决定了因子质量的上限
2. 没有"理解"和"进化"能力 — 当前调度器只看 Sharpe/Fitness/Turnover
   三个数做决策，不知道因子为什么好或为什么差，也无法基于经济逻辑提出改进方向

Agent 系统架构设计

我建议架构遵循 "LLM 作为大脑，规则引擎作为四肢" 的混合设计，分成 4 层：

┌─────────────────────────────────────────────────────────┐
│                  Agent Orchestrator                      │
│  (主循环: Idea → Probe → Analyze → Decide → Act → Loop) │
├─────────────────────────────────────────────────────────┤
│                                                          │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────┐  │
│  │ LLM Planner  │  │ LLM Analyst  │  │ LLM Mutator    │  │
│  │ (idea→templ) │  │ (results→    │  │ (factor+diag→  │  │
│  │              │  │  insights)   │  │  modifications) │  │
│  └─────────────┘  └──────────────┘  └────────────────┘  │
│                                                          │
│  ┌──────────────────────────────────────────────────┐    │
│  │          Rule-Based Execution Layer               │    │
│  │  (main.py, backtest_runner.py, datafields_store)  │    │
│  └──────────────────────────────────────────────────┘    │
│                                                          │
│  ┌──────────────────────────────────────────────────┐    │
│  │      知识层: Factor Knowledge Base / Memory       │    │
│  └──────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘

核心模块说明

1. LLM Planner — 将金融直觉转化为因子

- 输入：自然语言的 idea（如"期权隐含波动率偏度对收益的预测"）
- 输出：模板定义（Template JSON）或直接生成表达式
- 需要了解：WQ 算子语义、数据字段、金融逻辑

2. LLM Analyst — 深入诊断因子表现

- 输入：因子表达式 + 完整的回测结果（不仅仅是 Sharpe）
- 分析维度：
  - 收益特征：收益是否稳定？是否集中在某些时段？
  - 换手率来源：是否因为噪声交易？还是因为信号本身变化快？
  - 因子逻辑：因子表达式的金融逻辑是否自洽？是否可能存在数据泄露？
  - 相关性：与其他核心因子的相关性如何？
- 输出：结构化的诊断报告 + 具体的改进方向

3. LLM Mutator — 基于诊断进行定向变异

- 不随机尝试算子，而是根据诊断结果定向改进：
  - 换手率太高 → 增加平滑（ts_mean/ts_decay_linear）
  - Sharpe 波动大 → 增加分组调整（group_neutralize）
  - 收益分布偏斜 → 尝试 rank/zscore 变换
  - 因子逻辑简单 → 增加条件逻辑（if_else）、组合

4. Rule-Based Execution Layer — 你现有的代码几乎不用改

- main.py、backtest_runner.py、datafields_store.py 保持不动
- adaptive_scheduler.py 需要扩展以支持 Mutate 决策类型
- 添加 LLM 输出验证层（防止幻觉算子名）

5. Factor Knowledge Base — 持续积累的知识

- 每次迭代记录：因子表达式 → 回测指标 → 诊断 → 改进尝试 → 效果
- 长期来看可以学习：哪些算子组合在哪些数据集上有效
- 为 LLM 提供 RAG 上下文，减少幻觉

从 Idea 到优秀因子的完整循环

用户想法："我想测试 IV skew 对收益的预测能力"

│
▼
[1. LLM Planner: 生成模板或表达式]
│  → TPL_OPTION_IVSKEW_V1:
│    expression: group_rank(ts_zscore(subtract(<iv_call>, <iv_put>), <d>), <group>)
│
▼
[2. Probe Backtest (现有代码)]
│  提交代表性参数组合进行快速回测
│
▼
[3. LLM Analyst: 深度诊断]
│  → 发现: Sharpe ≈ 0.6, Turnover 超高
│  → 诊断: "IV spread 信号本身变化快，导致高频换手"
│  → 建议: 尝试 ts_decay_linear 平滑，或加 group_scale
│
▼
[4. LLM Mutator: 生成改进版本]
│  → Mutant 1: group_rank(ts_decay_linear(ts_zscore(...), 5), <group>)
│  → Mutant 2: group_rank(ts_mean(ts_zscore(...), 10), <group>)
│
▼
[5. 循环 (回到步骤 2)]
│  对 Mutant 1, 2 进行 Probe → Analyze → ...
│  直到:
│    - Sharpe ≥ 1.5 且 Fitness ≥ 1.0 → FINALIZE（报告优秀因子）
│    - 连续 3 轮无明显改善 → ABANDON（记录教训）
│    - 用户介入请求
│
▼
[6. Expand (现有代码)]
│  对达到标准的因子进行全参数扩展

实施路线图

我建议分 4 个阶段实施，每个阶段都有独立的交付价值：

---

阶段一：LLM 集成基础 + 结果深度分析（建议 1-2 周）

目标：先用 LLM 读懂回测结果，提供可操作的诊断

需要新建的文件：

- agent_llm_client.py — 统一 LLM API 客户端（Claude API），处理 prompt 模板、token 管理、重试
- result_analyzer.py — 深度结果分析，读取回测结果目录，对每个核心生成诊断报告
- agent_feedback_loop.py — 简单的交互式循环：分析结果 → 打印诊断 → 让用户选择下一步

阶段一的价值：

- 你可以立即看到 LLM 对各个因子的深入分析（而不只是三个数字）
- 诊断结果可以帮助你手动改进模板和参数选择
- 建立 LLM 集成的基础设施，后续阶段直接复用

# agent_llm_client.py 核心设计

class LLMClient:
def __init__(self, api_key: str, model: str = "claude-sonnet-4-20250506"):
self.client = anthropic.Anthropic(api_key=api_key)

  def analyze_factor_results(self,
                              factor_expression: str,
                              results: dict,
                              context: FactorContext
                             ) -> FactorDiagnosis:
      """分析单因子的回测结果，返回结构化诊断"""
      prompt = self._build_analysis_prompt(factor_expression, results, context)
      response = self.client.messages.create(...)
      return self._parse_diagnosis(response)
---

阶段二：模板自动生成 + 定向变异（建议 2-3 周）

目标：让 LLM 能够生成新模板，并能对现有因子提出改进方案

需要新建的文件：

- template_generator.py — 根据金融直觉生成模板定义，验证数据集字段存在性
- mutation_engine.py — 根据分析诊断生成因子变异版本
- expression_validator.py — 验证 LLM 生成的表达式：算子名是否合法、字段是否存在、语法是否正确

需要修改的文件：

- adaptive_scheduler.py — 增加 MUTATE 决策类型（除了 EXPAND/WATCH/ABANDON）
- main.py — 接受 LLM 动态生成的模板作为输入（扩展 template-ids 来源）

# mutation_engine.py 核心设计

class MutationEngine:
DIAGNOSIS_ACTION_MAP = {
"high_turnover": [
"Wrap the expression with ts_mean(X, <d>)",
"Wrap with ts_decay_linear(X, <d>)",
"Add group_scale before the output",
],
"low_sharpe_volatile": [
"Try group_neutralize instead of group_rank",
"Increase ts_zscore window",
"Add if_else condition to filter outliers",
],
"poor_monotonicity": [
"Replace group_rank with group_zscore",
"Add inverse or log transform",
],
}

  def suggest_mutations(self,
                        expression: str,
                        diagnosis: FactorDiagnosis
                       ) -> list[MutationSuggestion]:
      """根据诊断结果提出具体变异方案"""
      ...
阶段二的价值：

- 首次实现 "因子自动进化" 的能力
- 同一个核心（core）可以在多个方向上尝试改进
- 不再受限于已有的 26 个模板

---

阶段三：全自动闭环搜索（建议 3-4 周）

目标：从 idea 到优秀因子完全自动化

需要新建的文件：

- factor_agent.py — 主代理循环（Orchestrator）
- agent_memory.py — 因子记忆系统，记录每轮迭代
- convergence_detector.py — 检测何时因子已达最优或已无改进空间

需要修改的文件：

- run_pipeline.py — 集成 agent 模式（--agent 参数）
- 所有之前的模块整合为完整的管道

factor_agent.py 主循环伪代码：

def run_agent_loop(initial_idea: str, dataset_id: str, max_iterations: int):
memory = AgentMemory(dataset_id)
template = llm_planner.idea_to_template(initial_idea, dataset_id)

  for iteration in range(max_iterations):
      # 1. Probe
      probe_batch = generate_probe_batch(template, dataset_id)
      probe_results = run_backtest(probe_batch)

      # 2. Analyze each core
      for core_id, core_results in aggregate_by_core(probe_results):
          diagnosis = llm_analyzer.analyze(core_id, core_results)

          # 3. Decide
          decision = agent_decide(diagnosis, memory)

          if decision.type == "EXPAND":
              expand_batch = generate_expand(core_id)
              expand_results = run_backtest(expand_batch)
              if meets_excellent_criteria(expand_results):
                  report_excellent_factor(core_id, expand_results)
          elif decision.type == "MUTATE":
              new_template = llm_mutator.mutate(core_id, diagnosis)
              memory.record_mutation(core_id, new_template)
              add_to_next_round(new_template)
          elif decision.type == "ABANDON":
              memory.record_abandon(core_id, diagnosis.reason)
          elif decision.type == "FINALIZE":
              report_excellent_factor(core_id, diagnosis)

      # 4. Check global convergence
      if should_stop(memory, max_iterations):
          break

  return memory.generate_final_report()
阶段三的价值：

- 真正的"全栈因子搜索 Agent"
- 可以持续运行，自主探索因子空间
- 你只需要提供研究方向/idea，Agent 负责执行

---

阶段四：高级能力（持续优化）

- Template-free 生成：跳过模板，直接用 LLM 组合 WQ 算子和字段生成表达式
- 跨数据集学习：在一个数据集上学到的有效模式，迁移到其他数据集
- 因子组合发现：自动发现低相关的优秀因子并进行组合测试
- 研报集成：读取金融研报 PDF，自动提取因子 idea 并生成模板

---

成本与风险管理

┌─────────────────────────────────┬────────────────────────────────────────────────────────────┐
│              风险               │                            对策                            │
├─────────────────────────────────┼────────────────────────────────────────────────────────────┤
│ LLM                             │ 严格的 Expression Validator 层，校验所有 LLM 输出          │
│ 幻觉（编造不存在的算子/字段）   │                                                            │
├─────────────────────────────────┼────────────────────────────────────────────────────────────┤
│                                 │ 批量分析（一次分析一个 core                                │
│ LLM API 费用过高                │ 的所有回测，而非单个因子）；只在 WATCH 和 EXPAND 阶段调    │
│                                 │ LLM                                                        │
├─────────────────────────────────┼────────────────────────────────────────────────────────────┤
│ Brain API 日配额耗尽            │ Agent 跟踪每日使用量；ABANDON 决策要果断；每轮迭代控制     │
│                                 │ probe 规模                                                 │
├─────────────────────────────────┼────────────────────────────────────────────────────────────┤
│ 无限循环（因子永远改进不上去）  │ max_iterations 硬限制；收敛检测器；连续 N 轮无改善 →       │
│                                 │ ABANDON                                                    │
└─────────────────────────────────┴────────────────────────────────────────────────────────────┘

我建议的下一步

先做阶段一的 result_analyzer.py，这只需要 2-3 天的工作量，但能立刻让你感受到价值——你会看到 LLM
对因子的深度分析，而不是只看三个数字。同时建立 LLM 客户端基础设施。


claude --resume 9961cff1-a5c4-4cc2-87ce-35ff0d948943