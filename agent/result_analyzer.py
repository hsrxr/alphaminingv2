"""
result_analyzer.py — Deep analysis of backtest results using LLM.

Takes probe backtest results and produces structured diagnostic reports per
core, including economic interpretation, strength/weakness assessment, and
concrete improvement suggestions.

Workflow
────────
  1. Load probe results (reuses ``adaptive_scheduler.load_probe_results``).
  2. Aggregate by ``pipeline_core_id``.
  3. For each core above *min_sharpe*, call LLM for structured diagnosis.
  4. Save comprehensive analysis report + per-core diagnosis files.

Usage
─────
  python result_analyzer.py \\
      --dataset-id option8 \\
      --probe-results-dir backtest_results/probe \\
      --min-sharpe 0.3 \\
      --max-cores 20
"""

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path

from agent.llm_client import LLMClient, load_api_key


# ─────────────────────────────────────────────────────────────────────────────
#  Prompt management
# ─────────────────────────────────────────────────────────────────────────────

def _load_operator_context() -> str:
    """Load a concise summary of available WQ operators for LLM context."""
    try:
        with open("wq_operators_cleaned.json", encoding="utf-8") as fh:
            ops = json.load(fh)
    except (OSError, json.JSONDecodeError):
        return ""

    categories: dict[str, list[str]] = {}
    for op in ops:
        cat = op.get("category") or op.get("type", "General")
        name = op.get("operator_name") or op.get("operator_syntax", "").split("(")[0].strip()
        categories.setdefault(cat, []).append(name)

    lines = ["Available WorldQuant operator categories:"]
    for cat in sorted(categories):
        names = sorted(set(categories[cat]))[:12]
        lines.append(f"  {cat}: {', '.join(names)}")
    return "\n".join(lines)


_OPERATOR_CONTEXT_CACHE: str | None = None


def _get_operator_context() -> str:
    global _OPERATOR_CONTEXT_CACHE
    if _OPERATOR_CONTEXT_CACHE is None:
        _OPERATOR_CONTEXT_CACHE = _load_operator_context()
    return _OPERATOR_CONTEXT_CACHE


ANALYSIS_SYSTEM_PROMPT = """\
You are an expert quantitative analyst specializing in alpha factor research on \
the WorldQuant Brain platform. You deeply understand financial mathematics, \
factor investing, and the WQ FASTEXPR expression language.

Your task is to analyze factor backtest results and deliver a structured \
diagnosis that includes:
1. A clear, plain-English interpretation of the financial or econometric signal \
   the factor captures.
2. Specific, evidence-backed strengths and weaknesses of the factor's performance.
3. Concrete, actionable improvement suggestions (operator changes, smoothing, \
   normalization, grouping adjustments, etc.).
4. The economic principle that explains why this factor could predict returns.
5. An overall verdict on whether this factor line is worth pursuing further.

Always respond in valid JSON only — no markdown, no extra text outside the JSON.\
"""


def build_analysis_messages(
    expression: str,
    metrics: dict,
    template_id: str,
    dataset_id: str,
) -> list[dict]:
    """Build the LLM message list for a single-core analysis."""
    op_ctx = _get_operator_context()

    metrics_block = "\n".join(
        f"- {k}: {v:.4f}" if isinstance(v, float) else f"- {k}: {v}"
        for k, v in sorted(metrics.items())
        if v is not None
    )

    user = f"""\
Analyse this alpha factor.

## Factor Expression
{expression}

## Template
{template_id}

## Dataset
{dataset_id}

## Aggregated Probe Metrics
{metrics_block}

## Operator Context
{op_ctx}

## Instructions
Return a JSON object with these fields (and ONLY these fields):

{{
  "interpretation": "What financial/econometric signal does this factor capture?",
  "strengths": ["positive aspect 1", "positive aspect 2"],
  "weaknesses": ["issue 1", "issue 2"],
  "improvement_suggestions": [
    {{
      "description": "What to change and why",
      "expected_impact": "How this would affect Sharpe / Turnover / Fitness",
      "technique": "operator_change | add_smoothing | add_normalization | change_grouping | other"
    }}
  ],
  "economic_rationale": "The economic principle behind why this factor might predict returns",
  "verdict": "promising",
  "verdict_reason": "One-sentence justification"
}}

Verdict options:
- "promising": Sharpe ≥ 0.8 with reasonable turnover — worth expanding
- "mediocre": Sharpe ~0.5–0.8 or fixable issues — worth trying mutations
- "abandon": Sharpe < 0.5 or fundamental logical flaws

Respond with the raw JSON object only — no markdown fences, no commentary.\
"""

    return [
        {"role": "system", "content": ANALYSIS_SYSTEM_PROMPT},
        {"role": "user", "content": user},
    ]


# ─────────────────────────────────────────────────────────────────────────────
#  Core diagnosis
# ─────────────────────────────────────────────────────────────────────────────

def _safe_metrics(stats: dict) -> dict:
    """Flatten aggregate stats into a display-friendly dict."""
    return {
        k: stats[k]
        for k in (
            "sharpe_mean", "sharpe_max", "sharpe_min",
            "fitness_mean", "turnover_mean", "probe_count",
        )
        if k in stats
    }


def analyze_core(
    core_id: str,
    stats: dict,
    dataset_id: str,
    llm: LLMClient,
    quiet: bool = False,
) -> dict:
    """Run LLM analysis on a single core and return a structured diagnosis.

    Returns a dict that merges core metadata with the LLM response.
    """
    expression = stats.get("best_expression", "")
    template_id = stats.get("pipeline_template_id", "")

    messages = build_analysis_messages(
        expression=expression,
        metrics=_safe_metrics(stats),
        template_id=template_id,
        dataset_id=dataset_id,
    )

    if not quiet:
        print(f"    Calling LLM for {core_id[:60]}...", end=" ", flush=True)

    t0 = time.time()
    try:
        analysis = llm.chat_structured(messages)
        llm_ok = True
    except Exception as exc:
        analysis = {
            "interpretation": f"[LLM analysis failed: {exc}]",
            "strengths": [],
            "weaknesses": ["Analysis error"],
            "improvement_suggestions": [],
            "economic_rationale": "",
            "verdict": "error",
            "verdict_reason": str(exc),
        }
        llm_ok = False

    elapsed = time.time() - t0
    if not quiet:
        verdict = analysis.get("verdict", "?")
        print(f"done ({elapsed:.1f}s) → verdict: {verdict}")

    return {
        "core_id": core_id,
        "template_id": template_id,
        "expression": expression,
        "metrics": _safe_metrics(stats),
        "analysis": analysis,
        "llm_status": "ok" if llm_ok else "error",
        "analyzed_at": datetime.now().isoformat(timespec="seconds"),
    }


# ─────────────────────────────────────────────────────────────────────────────
#  Batch analysis
# ─────────────────────────────────────────────────────────────────────────────

def analyze_probe_results(
    probe_results_dir: str,
    dataset_id: str,
    min_sharpe: float = 0.3,
    max_cores: int = 20,
    output_dir: str = "analysis_reports",
    llm: LLMClient | None = None,
    quiet: bool = False,
) -> list[dict]:
    """Load probe results, aggregate by core, and analyze each with LLM.

    Returns a list of per-core diagnosis dicts.
    """
    from pipeline.adaptive_scheduler import load_probe_results, aggregate_by_core

    # 1. Load & aggregate ──────────────────────────────────────────────────
    results_dir = Path(probe_results_dir)
    if not results_dir.exists():
        raise FileNotFoundError(f"Probe results directory not found: {results_dir}")

    all_results = load_probe_results(results_dir)
    if not all_results:
        print("[WARN] No probe results loaded — nothing to analyze.")
        return []

    core_stats = aggregate_by_core(all_results)
    if not quiet:
        print(f"Loaded {len(all_results)} factors across {len(core_stats)} unique cores.")

    # 2. Filter & sort ─────────────────────────────────────────────────────
    candidates = [
        (cid, stats) for cid, stats in core_stats.items()
        if stats.get("sharpe_mean", 0) is not None
        and stats["sharpe_mean"] >= min_sharpe
    ]
    candidates.sort(key=lambda x: -x[1]["sharpe_mean"])

    selected = candidates[:max_cores]
    if not quiet:
        print(f"Analyzing {len(selected)} core(s) with Sharpe ≥ {min_sharpe} "
              f"(from {len(candidates)} candidates).")

    if not selected:
        print("[INFO] No cores meet the minimum Sharpe threshold.")
        return []

    # 3. LLM analysis ──────────────────────────────────────────────────────
    if llm is None:
        llm = LLMClient()

    diagnoses: list[dict] = []
    for idx, (core_id, stats) in enumerate(selected, 1):
        if not quiet:
            print(f"\n  [{idx}/{len(selected)}] {core_id[:70]}")
        diagnosis = analyze_core(
            core_id=core_id,
            stats=stats,
            dataset_id=dataset_id,
            llm=llm,
            quiet=quiet,
        )
        diagnoses.append(diagnosis)

    # 4. Build report & save ───────────────────────────────────────────────
    report = _build_report(diagnoses, dataset_id, probe_results_dir, min_sharpe)
    _save_report(report, diagnoses, output_dir, dataset_id)

    return diagnoses


def _build_report(
    diagnoses: list[dict],
    dataset_id: str,
    probe_results_dir: str,
    min_sharpe: float,
) -> dict:
    return {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "dataset_id": dataset_id,
        "probe_results_dir": probe_results_dir,
        "config": {"min_sharpe": min_sharpe, "total_analyzed": len(diagnoses)},
        "summary": {
            "total_analyzed": len(diagnoses),
            "verdicts": dict(
                _count_verdicts(diagnoses)
            ),
        },
        "diagnoses": [
            {k: d[k] for k in ("core_id", "template_id", "analysis", "llm_status")}
            for d in diagnoses
        ],
    }


def _count_verdicts(diagnoses: list[dict]) -> list[tuple[str, int]]:
    from collections import Counter
    verdicts = Counter(
        d.get("analysis", {}).get("verdict", "unknown") for d in diagnoses
    )
    return verdicts.most_common()


def _save_report(
    report: dict,
    diagnoses: list[dict],
    output_dir: str,
    dataset_id: str,
) -> Path:
    out_root = Path(output_dir) / dataset_id
    run_dir = out_root / datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    # Full report.
    report_path = run_dir / "analysis_report.json"
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    # Per-core diagnosis files.
    for d in diagnoses:
        core_slug = d["core_id"].replace("|", "_").replace("=", "_")[:80]
        (run_dir / f"diagnosis_{core_slug}.json").write_text(
            json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    print(f"\nAnalysis report saved to {report_path}")
    return report_path


# ─────────────────────────────────────────────────────────────────────────────
#  CLI
# ─────────────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Analyze probe backtest results using LLM. "
            "Loads results, aggregates by core, and produces structured diagnoses."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--dataset-id", required=True, help="Dataset id (e.g. option8, pv13).")
    parser.add_argument(
        "--probe-results-dir", default="backtest_results/probe",
        help="Directory containing probe backtest result files.",
    )
    parser.add_argument(
        "--min-sharpe", type=float, default=0.3,
        help="Minimum mean Sharpe for a core to be analyzed.",
    )
    parser.add_argument(
        "--max-cores", type=int, default=20,
        help="Maximum number of top cores to analyze.",
    )
    parser.add_argument(
        "--output-dir", default="analysis_reports",
        help="Directory to write analysis reports into.",
    )
    parser.add_argument(
        "--api-key", default="",
        help="DeepSeek API key (falls back to DEEPSEEK_API_KEY env / .env).",
    )
    parser.add_argument(
        "--model", default="deepseek-chat",
        help="LLM model name.",
    )
    parser.add_argument(
        "--quiet", action="store_true", default=False,
        help="Suppress per-core progress output.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    api_key = args.api_key or load_api_key()
    llm = LLMClient(api_key=api_key, model=args.model)

    print(f"=== Result Analyzer: dataset={args.dataset_id} ===\n")

    t0 = time.time()
    diagnoses = analyze_probe_results(
        probe_results_dir=args.probe_results_dir,
        dataset_id=args.dataset_id,
        min_sharpe=args.min_sharpe,
        max_cores=args.max_cores,
        output_dir=args.output_dir,
        llm=llm,
        quiet=args.quiet,
    )

    elapsed = time.time() - t0
    print(f"\nDone. Analyzed {len(diagnoses)} core(s) in {elapsed:.1f}s.")

    # Print a quick summary table to stdout.
    if diagnoses:
        print(f"\n{'Verdict':<14} {'Sharpe':>8} {'Core ID':<60}")
        print("-" * 82)
        for d in diagnoses:
            verdict = d["analysis"].get("verdict", "?")
            sharpe = d["metrics"].get("sharpe_mean", 0)
            core_short = d["core_id"][:60]
            print(f"{verdict:<14} {sharpe:>8.3f} {core_short:<60}")


if __name__ == "__main__":
    main()
