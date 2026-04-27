"""
分析 LLM Agent 在 alphaminingv2 各阶段的价值与风险，生成架构对比图
"""
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch
import matplotlib.font_manager as fm

# 设置中文字体
plt.rcParams['font.family'] = ['DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

# ─────────────────────────────────────────────
# 图1：当前架构 vs LLM Agent 架构的流程对比
# ─────────────────────────────────────────────

fig, axes = plt.subplots(1, 2, figsize=(18, 10))
fig.patch.set_facecolor('#0f1117')

def draw_pipeline(ax, title, stages, arrows, color_scheme):
    ax.set_facecolor('#0f1117')
    ax.set_xlim(0, 10)
    ax.set_ylim(0, len(stages) * 2 + 1)
    ax.axis('off')
    ax.set_title(title, color='white', fontsize=14, fontweight='bold', pad=15)

    for i, (stage_name, detail, color) in enumerate(stages):
        y = len(stages) * 2 - i * 2
        box = FancyBboxPatch((0.5, y - 0.6), 9, 1.1,
                              boxstyle="round,pad=0.1",
                              facecolor=color, edgecolor='white', linewidth=1.2, alpha=0.85)
        ax.add_patch(box)
        ax.text(5, y, stage_name, ha='center', va='center',
                color='white', fontsize=11, fontweight='bold')
        ax.text(5, y - 0.35, detail, ha='center', va='center',
                color='#cccccc', fontsize=8)

        if i < len(stages) - 1:
            arrow_color, arrow_label = arrows[i]
            ax.annotate('', xy=(5, y - 0.65), xytext=(5, y - 1.35),
                        arrowprops=dict(arrowstyle='->', color=arrow_color, lw=2))
            ax.text(5.3, y - 1.0, arrow_label, color=arrow_color, fontsize=8, va='center')

# 当前架构
current_stages = [
    ("main.py\nFactor Generator", "Cartesian product over ALL slots", '#1a3a5c'),
    ("factor_batches/\nJSON Files", "5000 factors, no core metadata", '#1a3a5c'),
    ("backtest_runner.py\nBlind Consumer", "Sequential, no early stopping", '#3a1a1a'),
    ("backtest_results/\nRaw Results", "Per-factor JSON, no aggregation", '#1a3a5c'),
    ("result_filter.py\nPost-filter", "Static threshold, no feedback", '#3a1a1a'),
]
current_arrows = [
    ('#ff6b6b', 'Exhaustive expansion'),
    ('#888888', 'Dump & forget'),
    ('#ff6b6b', 'No early stop'),
    ('#888888', 'One-way'),
]

draw_pipeline(axes[0], "Current Architecture (Open-Loop)", current_stages, current_arrows, None)

# LLM Agent 架构
agent_stages = [
    ("LLM Planner\n(Strategy Brain)", "Hypothesis generation, template design", '#1a4a2a'),
    ("Probe Batch\n(Representative Params)", "3 params per core, not 28", '#1a4a2a'),
    ("backtest_runner.py\n(Unchanged)", "Execute 3 probes per core", '#1a3a5c'),
    ("LLM Evaluator\n(Result Interpreter)", "Classify core: expand / mutate / abandon", '#1a4a2a'),
    ("Adaptive Scheduler\n(Closed-Loop)", "Expand winners, mutate near-misses", '#1a4a2a'),
]
agent_arrows = [
    ('#4ade80', 'Targeted generation'),
    ('#4ade80', 'Minimal probes'),
    ('#4ade80', 'Feedback loop'),
    ('#4ade80', 'Adaptive decision'),
]

draw_pipeline(axes[1], "LLM Agent Architecture (Closed-Loop)", agent_stages, agent_arrows, None)

plt.tight_layout(pad=2)
plt.savefig('/home/ubuntu/alphaminingv2/pipeline_comparison.png', dpi=150, bbox_inches='tight',
            facecolor='#0f1117')
plt.close()
print("Saved pipeline_comparison.png")


# ─────────────────────────────────────────────
# 图2：LLM Agent 各阶段的价值/风险/适合度矩阵
# ─────────────────────────────────────────────

fig, ax = plt.subplots(figsize=(14, 8))
fig.patch.set_facecolor('#0f1117')
ax.set_facecolor('#0f1117')

tasks = [
    "Template\nDesign",
    "Field\nSelection",
    "Parameter\nPriority",
    "Result\nInterpretation",
    "Core\nEval & Prune",
    "Mutation\n& Composition",
    "Submission\nScheduling",
]

# 各维度评分 (0-10)
llm_value    = [9,  7,  8,  8,  7,  9,  3]   # LLM 能带来的价值
rule_value   = [3,  6,  5,  4,  6,  2,  9]   # 规则/代码能做到的程度
llm_risk     = [4,  6,  3,  5,  4,  7,  8]   # LLM 引入的风险
recommended  = [1,  0,  1,  1,  1,  1,  0]   # 是否建议用 LLM (1=yes, 0=no)

x = np.arange(len(tasks))
width = 0.28

bars1 = ax.bar(x - width, llm_value, width, label='LLM Value Added', color='#4ade80', alpha=0.85)
bars2 = ax.bar(x, rule_value, width, label='Rule-Based Capability', color='#60a5fa', alpha=0.85)
bars3 = ax.bar(x + width, llm_risk, width, label='LLM Risk Level', color='#f87171', alpha=0.85)

# 标注推荐/不推荐
for i, rec in enumerate(recommended):
    label = 'LLM' if rec else 'Rule'
    color = '#4ade80' if rec else '#f87171'
    ax.text(x[i], 10.5, label, ha='center', va='bottom', color=color,
            fontsize=9, fontweight='bold')

ax.set_xticks(x)
ax.set_xticklabels(tasks, color='white', fontsize=10)
ax.set_ylim(0, 12)
ax.set_ylabel('Score (0-10)', color='white', fontsize=11)
ax.set_title('LLM Agent Value vs Risk by Pipeline Stage', color='white', fontsize=14, fontweight='bold', pad=20)
ax.tick_params(colors='white')
ax.spines['bottom'].set_color('#444')
ax.spines['left'].set_color('#444')
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.yaxis.label.set_color('white')
ax.legend(loc='upper right', facecolor='#1a1a2e', edgecolor='#444', labelcolor='white', fontsize=10)

ax.text(0.5, -0.12, 'Green label = Recommend LLM  |  Red label = Recommend Rule-Based',
        transform=ax.transAxes, ha='center', color='#aaaaaa', fontsize=9)

plt.tight_layout()
plt.savefig('/home/ubuntu/alphaminingv2/llm_value_risk_matrix.png', dpi=150, bbox_inches='tight',
            facecolor='#0f1117')
plt.close()
print("Saved llm_value_risk_matrix.png")


# ─────────────────────────────────────────────
# 图3：搜索效率对比（当前 vs Probe-Expand vs LLM Agent）
# ─────────────────────────────────────────────

fig, axes = plt.subplots(1, 2, figsize=(14, 6))
fig.patch.set_facecolor('#0f1117')

# 左图：每个 Core 的回测次数
ax = axes[0]
ax.set_facecolor('#0f1117')
methods = ['Current\n(Brute Force)', 'Probe-Expand\n(Rule-Based)', 'LLM Agent\n(Adaptive)']
backtests_per_core = [28, 12, 5]
colors = ['#f87171', '#fbbf24', '#4ade80']
bars = ax.bar(methods, backtests_per_core, color=colors, alpha=0.85, width=0.5)
for bar, val in zip(bars, backtests_per_core):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.3,
            f'{val}x', ha='center', va='bottom', color='white', fontsize=13, fontweight='bold')
ax.set_ylabel('Backtests per Core', color='white', fontsize=11)
ax.set_title('Backtest Cost per Core', color='white', fontsize=12, fontweight='bold')
ax.tick_params(colors='white')
ax.spines['bottom'].set_color('#444')
ax.spines['left'].set_color('#444')
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)
ax.set_ylim(0, 35)

# 右图：1000个 Core 的总回测量对比
ax = axes[1]
ax.set_facecolor('#0f1117')
n_cores = 1000
total = [28 * n_cores, 12 * n_cores * 0.3 + 28 * n_cores * 0.1,
         5 * n_cores * 0.3 + 15 * n_cores * 0.15]
labels = ['Current\n(All Cores Full)', 'Probe-Expand\n(30% expand)', 'LLM Agent\n(15% expand)']
bars = ax.bar(labels, total, color=colors, alpha=0.85, width=0.5)
for bar, val in zip(bars, total):
    ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 100,
            f'{int(val):,}', ha='center', va='bottom', color='white', fontsize=11, fontweight='bold')
ax.set_ylabel('Total Backtests (1000 Cores)', color='white', fontsize=11)
ax.set_title('Total Backtest Budget (1000 Cores)', color='white', fontsize=12, fontweight='bold')
ax.tick_params(colors='white')
ax.spines['bottom'].set_color('#444')
ax.spines['left'].set_color('#444')
ax.spines['top'].set_visible(False)
ax.spines['right'].set_visible(False)

plt.tight_layout(pad=2)
plt.savefig('/home/ubuntu/alphaminingv2/efficiency_comparison.png', dpi=150, bbox_inches='tight',
            facecolor='#0f1117')
plt.close()
print("Saved efficiency_comparison.png")
