"""
agent_feedback_loop.py — Interactive agent loop for factor analysis & decision-making.

Workflow
────────
  1. Load probe backtest results.
  2. Aggregate by core, filter by minimum Sharpe.
  3. For each qualifying core:
     a. Call LLM for structured diagnosis.
     b. Present diagnosis to the user.
     c. User decides: [E]xpand, [M]utate, [A]bandon, [S]kip, [Q]uit.
     d. Record decision.
  4. Save full report with all decisions.

Usage
─────
  python agent_feedback_loop.py --dataset-id option8

The loop can be resumed: previously analyzed cores are loaded from the output
directory and skipped unless ``--force`` is passed.
"""

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path

from agent.llm_client import LLMClient, load_api_key
from agent.result_analyzer import analyze_core, analyze_probe_results


# ─── ANSI helpers ───────────────────────────────────────────────────────────

def _c(code: str, text: str) -> str:
    return f"{code}{text}\033[0m" if sys.stdout.isatty() else text


BOLD = "\033[1m"
DIM = "\033[2m"
GREEN = "\033[92m"
YELLOW = "\033[93m"
RED = "\033[91m"
CYAN = "\033[96m"
MAGENTA = "\033[95m"


# ─── Decision constants ─────────────────────────────────────────────────────

DECISION_EXPAND = "EXPAND"
DECISION_MUTATE = "MUTATE"
DECISION_ABANDON = "ABANDON"
DECISION_SKIP = "SKIP"

DECISION_LABELS = {
    DECISION_EXPAND: _c(GREEN, "EXPAND"),
    DECISION_MUTATE: _c(YELLOW, "MUTATE"),
    DECISION_ABANDON: _c(RED, "ABANDON"),
    DECISION_SKIP: _c(DIM, "SKIP"),
}


# ─── State persistence ──────────────────────────────────────────────────────

class DecisionStore:
    """Persist and resume user decisions across sessions."""

    def __init__(self, path: Path):
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._decisions: dict[str, dict] = {}
        self._load()

    def _load(self) -> None:
        if self.path.exists():
            try:
                data = json.loads(self.path.read_text(encoding="utf-8"))
                self._decisions = data.get("decisions", {})
            except (OSError, json.JSONDecodeError):
                self._decisions = {}

    def save(self) -> None:
        payload = {
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "decision_count": len(self._decisions),
            "decisions": self._decisions,
        }
        self.path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def has(self, core_id: str) -> bool:
        return core_id in self._decisions

    def get(self, core_id: str) -> str | None:
        entry = self._decisions.get(core_id)
        return entry.get("decision") if entry else None

    def put(self, core_id: str, decision: str, note: str = "") -> None:
        self._decisions[core_id] = {
            "decision": decision,
            "note": note,
            "decided_at": datetime.now().isoformat(timespec="seconds"),
        }
        self.save()


# ─── Interactive loop ───────────────────────────────────────────────────────

def _print_header(text: str) -> None:
    try:
        width = min(78, os.get_terminal_size().columns)
    except (OSError, ValueError):
        width = 78
    print()
    print("─" * width)
    print(f"  {text}")
    print("─" * width)


def _prompt_decision(core_id: str, diagnosis: dict) -> tuple[str, str]:
    """Present the diagnosis and ask the user for a decision."""
    analysis = diagnosis.get("analysis", {})
    metrics = diagnosis.get("metrics", {})

    # ── Display core info ──────────────────────────────────────────────
    print(BOLD + f"\nCore: {core_id}" + "\033[0m")
    print(f"  Expression: {diagnosis.get('expression', '')[:120]}")
    print(
        f"  Sharpe μ={metrics.get('sharpe_mean', 0):.3f}  "
        f"max={metrics.get('sharpe_max', 0):.3f}  "
        f"Fitness μ={metrics.get('fitness_mean', 0):.3f}  "
        f"Turnover μ={metrics.get('turnover_mean', 0):.3f}  "
        f"n={metrics.get('probe_count', 0)}"
    )

    # ── LLM analysis ───────────────────────────────────────────────────
    print(BOLD + "\n  LLM Analysis:" + "\033[0m")
    print(f"    Interpretation: {analysis.get('interpretation', 'N/A')[:200]}")
    print(f"    Economic rationale: {analysis.get('economic_rationale', 'N/A')[:200]}")

    if analysis.get("strengths"):
        print(BOLD + "    Strengths:" + "\033[0m")
        for s in analysis["strengths"]:
            print(f"      \033[92m✓\033[0m {s[:120]}")

    if analysis.get("weaknesses"):
        print(BOLD + "    Weaknesses:" + "\033[0m")
        for w in analysis["weaknesses"]:
            print(f"      \033[91m✗\033[0m {w[:120]}")

    if analysis.get("improvement_suggestions"):
        print(BOLD + "    Suggestions:" + "\033[0m")
        for i, sug in enumerate(analysis["improvement_suggestions"], 1):
            print(f"      {i}. {sug.get('description', '')[:150]}")
            if sug.get("expected_impact"):
                print(f"         → {sug['expected_impact'][:100]}")

    verdict = analysis.get("verdict", "?")
    verdict_color = {"promising": GREEN, "mediocre": YELLOW, "abandon": RED}.get(verdict, DIM)
    print(f"\n    Verdict: {_c(verdict_color, verdict.upper())}")
    print(f"    Reason: {analysis.get('verdict_reason', '')[:200]}")

    # ── Decision prompt ─────────────────────────────────────────────────
    print()
    while True:
        try:
            choice = input(
                "  Your decision  "
                "[E]xpand  [M]utate  [A]bandon  [S]kip  [Q]uit  [?] help\n"
                "  > "
            ).strip().upper()
        except (EOFError, KeyboardInterrupt):
            print()
            return DECISION_SKIP, ""

        if choice in ("E", "EXPAND"):
            return DECISION_EXPAND, ""
        if choice in ("M", "MUTATE"):
            note = input("  Mutation note (optional): ").strip()
            return DECISION_MUTATE, note
        if choice in ("A", "ABANDON"):
            note = input("  Reason for abandoning (optional): ").strip()
            return DECISION_ABANDON, note
        if choice in ("S", "SKIP"):
            return DECISION_SKIP, ""
        if choice in ("Q", "QUIT"):
            print("  Exiting loop.")
            raise KeyboardInterrupt()
        if choice in ("?", "HELP"):
            print("  E = EXPAND  →  Generate full parameter grid for this core")
            print("  M = MUTATE  →  Record that this core needs operator/expression mutations")
            print("  A = ABANDON →  Mark this core as not worth pursuing")
            print("  S = SKIP    →  Skip this core for now, leave it undecided")
            print("  Q = QUIT    →  Exit the interactive loop")
            print("  ? = HELP    →  Show this help")
            continue
        print("  Invalid choice. Enter E, M, A, S, Q, or ?.")


# ─── Main loop ──────────────────────────────────────────────────────────────

def run_feedback_loop(
    dataset_id: str,
    probe_results_dir: str = "backtest_results/probe",
    min_sharpe: float = 0.3,
    max_cores: int = 20,
    output_dir: str = "analysis_reports",
    llm: LLMClient | None = None,
    force: bool = False,
    auto_mode: str | None = None,
) -> list[dict]:
    """Run the interactive agent feedback loop.

    Parameters
    ----------
    dataset_id : str
        Dataset identifier used for display / metadata.
    probe_results_dir : str
        Path to probe backtest results.
    min_sharpe : float
        Minimum mean Sharpe for a core to be analyzed.
    max_cores : int
        Limit number of cores analyzed in one session.
    output_dir : str
        Directory for analysis reports and decisions.
    llm : LLMClient or None
        Reusable LLM client (created fresh if None).
    force : bool
        Re-analyze and re-prompt even if a decision already exists.
    auto_mode : str or None
        If set, automatically apply this decision to all cores (no interactive prompts).
        Values: "expand", "mutate", "abandon", "skip", or None for interactive.
    """
    from pipeline.adaptive_scheduler import load_probe_results, aggregate_by_core

    if llm is None:
        llm = LLMClient()

    # Set up paths.
    out_root = Path(output_dir) / dataset_id
    out_root.mkdir(parents=True, exist_ok=True)
    run_tag = datetime.now().strftime("%Y%m%d_%H%M%S")
    decisions_store = DecisionStore(out_root / "decisions.json")

    # ── Step 1: load & aggregate ────────────────────────────────────────
    _print_header(f"FACTOR AGENT — {dataset_id}  ({run_tag})")

    results_dir = Path(probe_results_dir)
    if not results_dir.exists():
        print(f"[ERROR] Probe results directory not found: {results_dir}")
        return []

    all_results = load_probe_results(results_dir)
    if not all_results:
        print("[ERROR] No probe results loaded.")
        return []

    core_stats = aggregate_by_core(all_results)
    print(f"Loaded {len(all_results)} factors across {len(core_stats)} unique cores.")

    # ── Step 2: filter & sort ───────────────────────────────────────────
    candidates = [
        (cid, stats) for cid, stats in core_stats.items()
        if stats.get("sharpe_mean", 0) is not None
        and stats["sharpe_mean"] >= min_sharpe
    ]
    candidates.sort(key=lambda x: -x[1]["sharpe_mean"])
    selected = candidates[:max_cores]
    print(f"Selected {len(selected)} core(s) with Sharpe ≥ {min_sharpe}.")

    if not selected:
        print("[INFO] No cores meet the minimum Sharpe threshold.")
        return []

    # ── Step 3: per-core analysis & decision ────────────────────────────
    decisions: list[dict] = []
    llm_calls = 0

    for idx, (core_id, stats) in enumerate(selected, 1):
        # Skip if already decided (unless --force).
        existing = decisions_store.get(core_id)
        if existing and not force:
            print(
                f"\n  [{idx}/{len(selected)}] {core_id[:60]} "
                f"→ already decided: {DECISION_LABELS.get(existing, existing)} "
                f"(use --force to re-analyze)"
            )
            decisions.append({
                "core_id": core_id,
                "template_id": stats.get("pipeline_template_id", ""),
                "decision": existing,
                "note": "",
                "skipped": True,
            })
            continue

        # LLM analysis.
        print(f"\n  [{idx}/{len(selected)}] {core_id[:70]}")
        diagnosis = analyze_core(
            core_id=core_id,
            stats=stats,
            dataset_id=dataset_id,
            llm=llm,
            quiet=False,
        )
        llm_calls += 1

        # Decision.
        if auto_mode:
            decision = auto_mode.upper()
            note = f"Auto-decision ({auto_mode})"
        else:
            try:
                decision, note = _prompt_decision(core_id, diagnosis)
            except KeyboardInterrupt:
                print("\nLoop interrupted by user.")
                break

        decisions_store.put(core_id, decision, note)
        decisions.append({
            "core_id": core_id,
            "template_id": stats.get("pipeline_template_id", ""),
            "decision": decision,
            "note": note,
            "expression": stats.get("best_expression", ""),
            "metrics": {
                "sharpe_mean": stats.get("sharpe_mean"),
                "sharpe_max": stats.get("sharpe_max"),
                "turnover_mean": stats.get("turnover_mean"),
                "probe_count": stats.get("probe_count"),
            },
            "analysis": diagnosis.get("analysis", {}),
        })

    # ── Step 4: summary & report ────────────────────────────────────────
    _print_header("SESSION SUMMARY")

    verdict_counts: dict[str, int] = {}
    for d in decisions:
        verdict_counts[d["decision"]] = verdict_counts.get(d["decision"], 0) + 1

    for dec, count in sorted(verdict_counts.items()):
        label = DECISION_LABELS.get(dec, dec)
        print(f"  {label}: {count}")

    print(f"\n  Total LLM calls: {llm_calls}")
    print(f"  Cores decided  : {len(decisions)}")

    report = {
        "session_id": run_tag,
        "dataset_id": dataset_id,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "config": {
            "min_sharpe": min_sharpe,
            "max_cores": max_cores,
            "auto_mode": auto_mode,
            "force": force,
        },
        "summary": dict(verdict_counts),
        "llm_calls": llm_calls,
        "decisions": decisions,
    }

    report_path = out_root / f"session_{run_tag}.json"
    report_path.write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    print(f"\nSession report saved to {report_path}")

    return decisions


# ─── CLI ────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Interactive agent loop for factor analysis and decision making.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--dataset-id", required=True, help="Dataset id (e.g. option8).")
    parser.add_argument(
        "--probe-results-dir", default="backtest_results/probe",
        help="Directory containing probe backtest result files.",
    )
    parser.add_argument(
        "--min-sharpe", type=float, default=0.3,
        help="Minimum mean Sharpe for a core to enter the loop.",
    )
    parser.add_argument(
        "--max-cores", type=int, default=20,
        help="Maximum number of top cores to process.",
    )
    parser.add_argument(
        "--output-dir", default="analysis_reports",
        help="Directory for analysis reports and decision logs.",
    )
    parser.add_argument("--api-key", default="", help="DeepSeek API key.")
    parser.add_argument("--model", default="deepseek-chat", help="LLM model name.")
    parser.add_argument(
        "--force", action="store_true", default=False,
        help="Re-analyze cores that already have decisions on file.",
    )
    parser.add_argument(
        "--auto", choices=["expand", "mutate", "abandon", "skip"], default=None,
        help="Non-interactive mode: auto-apply this decision to all cores.",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    api_key = args.api_key or load_api_key()
    llm = LLMClient(api_key=api_key, model=args.model)

    try:
        run_feedback_loop(
            dataset_id=args.dataset_id,
            probe_results_dir=args.probe_results_dir,
            min_sharpe=args.min_sharpe,
            max_cores=args.max_cores,
            output_dir=args.output_dir,
            llm=llm,
            force=args.force,
            auto_mode=args.auto,
        )
    except KeyboardInterrupt:
        print("\nAgent loop terminated by user.")
        sys.exit(0)


if __name__ == "__main__":
    main()
