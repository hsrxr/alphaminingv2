"""
agent/orchestrator.py — Full autonomous factor search agent.

Closes the loop:

    Idea → Template → Probe → Analyze → Decide
                                         ├─ EXPAND   → full grid backtest
                                         ├─ MUTATE   → mutate expression → re-probe
                                         ├─ ABANDON  → record & drop
                                         └─ FINALIZE → report excellent factor
                              ↑_____________|  (loop until convergence)

Usage
─────
  python -m agent.orchestrator \\
      --dataset-id option8 \\
      --idea "IV skew minus HV skew for volatility risk premium" \\
      --iterations 5
"""

import argparse
import json
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from agent.llm_client import LLMClient, load_api_key
from agent.expression_validator import ExpressionValidator
from agent.template_generator import TemplateGenerator
from agent.mutation_engine import MutationEngine
from agent.result_analyzer import analyze_probe_results, analyze_core
from agent.convergence import ConvergenceDetector
from agent.memory import FactorMemory

from pipeline.adaptive_scheduler import load_probe_results, aggregate_by_core


# ─── ANSI helpers ───────────────────────────────────────────────────────────

def _header(text: str) -> None:
    width = 72
    print()
    print("=" * width)
    print(f"  {text}")
    print("=" * width)


def _status(label: str, msg: str = "") -> None:
    print(f"  [{label}] {msg}")


# ─── Orchestrator ───────────────────────────────────────────────────────────

class FactorAgent:
    """Autonomous factor search agent — the main orchestrator."""

    def __init__(
        self,
        dataset_id: str,
        api_key: Optional[str] = None,
        model: str = "deepseek-chat",
        output_dir: str = "agent_output",
        mutate_mode: str = "rule",
        **kwargs,
    ):
        self.dataset_id = dataset_id
        self.output_dir = Path(output_dir)
        self.mutate_mode = mutate_mode
        self.quiet = kwargs.pop("quiet", False)

        # Sub-components.
        self.llm = LLMClient(api_key=api_key, model=model)
        self.validator = ExpressionValidator()
        self.mutation_engine = MutationEngine(validator=self.validator)
        self.convergence = ConvergenceDetector(
            max_rounds=kwargs.pop("max_rounds", 5),
            patience=kwargs.pop("patience", 2),
            excellent_sharpe=kwargs.pop("excellent_sharpe", 1.5),
        )
        self.memory = FactorMemory(storage_dir=str(self.output_dir / "memory"))

        # Run-time directories (set in run()).
        self._session_dir: Optional[Path] = None

    # ── public API ───────────────────────────────────────────────────────

    def run(
        self,
        idea: str,
        max_iterations: int = 5,
        skip_template: bool = False,
        template_path: str = "",
    ) -> dict:
        """Execute the full autonomous search loop.

        Parameters
        ----------
        idea : str
            Financial idea in plain English.
        max_iterations : int
            Maximum number of probe → analyze → mutate rounds.
        skip_template : bool
            Skip template generation and use an existing template file.
        template_path : str
            Path to existing template catalog (used when skip_template=True).

        Returns the session summary dict.
        """
        # ── Setup ────────────────────────────────────────────────────
        self.memory.init_session(self.dataset_id, idea)
        self._session_dir = self.output_dir / self.memory.session_id
        self._session_dir.mkdir(parents=True, exist_ok=True)

        template_dir = self._session_dir / "templates"
        batch_dir = self._session_dir / "batches"
        result_dir = self._session_dir / "results"
        template_dir.mkdir(parents=True, exist_ok=True)

        # Fresh template catalog path used each round.
        catalog_path = Path(template_path) if template_path else Path()

        if not skip_template:
            # ── Step 1: generate template from idea ────────────────
            _header(f"ROUND 0 — Template Generation")
            _status("LLM", f"Generating template from: \"{idea}\"")

            generator = TemplateGenerator(self.llm, self.validator)
            template = generator.idea_to_template(
                idea=idea,
                dataset_id=self.dataset_id,
                output_dir=str(template_dir),
                quiet=self.quiet,
            )
            if not template:
                _status("FAIL", "Template generation failed. Aborting.")
                return self.memory.summary()

            # Build a single-template catalog from the generated template.
            catalog = {
                "naming": {"regex": "^TPL_[A-Z0-9_]+_V[0-9]+$"},
                "templates": [template],
            }
            catalog_path = template_dir / f"catalog_{template['template_id']}.json"
            catalog_path.write_text(
                json.dumps(catalog, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            _status("TMPL", f"Template catalog: {catalog_path}")
        else:
            if not catalog_path.exists():
                _status("FAIL", f"Template file not found: {catalog_path}")
                return self.memory.summary()
            _status("TMPL", f"Using existing template: {catalog_path}")

        active_catalog = catalog_path

        # ── Main loop ────────────────────────────────────────────────────
        for round_num in range(1, max_iterations + 1):
            self.memory.next_round()
            _header(f"ROUND {round_num}/{max_iterations}")
            _status("MEM", f"Cores tracked: {len(self.memory._cores)} | "
                           f"Mutations: {self.memory._mutations_applied}")

            # Round-specific directories.
            round_batch = self._session_dir / f"batches/probe_round_{round_num}"
            round_result = self._session_dir / f"results/probe_round_{round_num}"
            round_batch.mkdir(parents=True, exist_ok=True)

            # ── a) Probe generation ────────────────────────────────
            _status("GEN", "Generating probe batch ...")
            probe_ok = self._run_probe_gen(active_catalog, round_batch)

            if not probe_ok:
                _status("SKIP", "Probe generation produced no factors.")
                continue

            # ── b) Probe backtest ──────────────────────────────────
            _status("BT",  "Running probe backtest ...")
            self._run_backtest(round_batch, round_result)

            # ── c) Analyze results ─────────────────────────────────
            _status("ANL", "Analyzing results with LLM ...")
            diagnoses = self._analyze(round_result)

            if not diagnoses:
                _status("SKIP", "No analyzable cores found.")
                continue

            # ── d) Per-core decision ───────────────────────────────
            next_catalog = {"naming": {"regex": "^TPL_[A-Z0-9_]+_V[0-9]+$"},
                            "templates": []}
            has_mutations = False

            for diag in diagnoses:
                core_id = diag["core_id"]
                expression = diag.get("expression", "")
                metrics = diag.get("metrics", {})

                # Record results in memory.
                self.memory.record_probe_result(
                    core_id, expression, metrics,
                    template_id=diag.get("template_id", ""),
                )
                self.memory.record_diagnosis(core_id, diag.get("analysis", {}))

                # Classify and decide.
                core_stats = {
                    "sharpe_mean": metrics.get("sharpe_mean", 0),
                    "turnover_mean": metrics.get("turnover_mean", 1),
                    "fitness_mean": metrics.get("fitness_mean", 0),
                }

                if self.convergence.should_expand(core_stats, self.memory, core_id):
                    self.memory.record_decision(core_id, "EXPAND")
                    _status("EXPAND", f"{core_id[:60]}")

                elif self.convergence.should_mutate(core_stats):
                    _status("MUTATE", f"{core_id[:60]}")
                    mutations = self._generate_mutations(
                        expression, diag.get("analysis", {}), core_stats,
                    )
                    for m in mutations:
                        if m.get("valid"):
                            self.memory.record_mutation(
                                core_id, expression,
                                m["mutation_id"], m["mutated_expression"],
                            )
                            next_catalog["templates"].append({
                                "template_id": f"TPL_MUT_{m['mutation_id'].upper()}_V1",
                                "source_name": m["mutation_id"],
                                "description": m["description"][:120],
                                "expression": m["mutated_expression"],
                                "core_slots": [],
                                "applicable_datasets": [self.dataset_id],
                                "slots": {},
                            })
                            has_mutations = True
                    self.memory.record_decision(core_id, "MUTATE")

                else:
                    self.memory.record_decision(core_id, "ABANDON")
                    _status("ABANDON", f"{core_id[:60]} (Sh=metrics.get('sharpe_mean', 0):.2f)")

                # Check for finalization.
                core_check = self.convergence.check_core(core_id, self.memory)
                if core_check == "finalize":
                    self.memory.record_decision(core_id, "FINALIZE")
                    _status("★ FINALIZE", f"{core_id[:60]} — excellent factor!")

            # ── e) Prepare next round ──────────────────────────────
            if has_mutations:
                mutations_path = self._session_dir / f"templates/mutations_round_{round_num}.json"
                mutations_path.write_text(
                    json.dumps(next_catalog, ensure_ascii=False, indent=2), encoding="utf-8"
                )
                active_catalog = mutations_path
                _status("NEXT", f"{len(next_catalog['templates'])} mutation(s) queued for round {round_num + 1}")
            else:
                # No mutations — check session convergence.
                if self.convergence.check_session(self.memory, round_num):
                    _status("DONE", "No active cores or max rounds reached.")
                    break
                # If there are expand decisions, run expand now.
                self._run_expand()

            # ── f) Expand any EXPANDED cores ───────────────────────
            if round_num == max_iterations or self.convergence.check_session(self.memory, round_num):
                self._run_expand()

        # ── Final report ────────────────────────────────────────────
        summary = self.memory.summary()
        self._write_report(summary)
        return summary

    # ── pipeline subprocess wrappers ────────────────────────────────────

    def _run_probe_gen(self, catalog_path: Path, output_dir: Path) -> bool:
        """Run main.py in probe mode.  Returns True if factors were generated."""
        cmd = [
            sys.executable, str(Path(__file__).resolve().parent.parent / "main.py"),
            "--dataset-id", self.dataset_id,
            "--template-doc", str(catalog_path),
            "--template-ids", "ALL",
            "--output-dir", str(output_dir),
            "--max-per-template", "5000",
            "--max-generated", "20000",
            "--probe",
        ]
        try:
            subprocess.run(cmd, check=True, capture_output=not self.quiet, timeout=300)
        except subprocess.CalledProcessError:
            return False
        except subprocess.TimeoutExpired:
            _status("WARN", "Probe generation timed out.")
            return False

        # Check if any batch files were created.
        batch_files = list(output_dir.rglob("*.json"))
        return len(batch_files) > 0

    def _run_backtest(self, input_dir: Path, output_dir: Path) -> None:
        """Run backtest_runner.py (once mode)."""
        cmd = [
            sys.executable, str(Path(__file__).resolve().parent.parent / "backtest_runner.py"),
            "--input-dir", str(input_dir),
            "--output-dir", str(output_dir),
            "--max-workers", "3",
            "--max-retries", "3",
            "--once",
        ]
        try:
            subprocess.run(cmd, check=True, capture_output=not self.quiet, timeout=1800)
        except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as exc:
            _status("WARN", f"Backtest subprocess issue: {exc}")

    def _run_expand(self) -> None:
        """Run expand for all EXPAND-marked cores."""
        expand_batch = self._session_dir / "batches/expand"
        expand_result = self._session_dir / "results/expand"
        expand_batch.mkdir(parents=True, exist_ok=True)

        cores_for_expand = [
            (cid, core) for cid, core in self.memory._cores.items()
            if core.get("decisions") and core["decisions"][-1]["decision"] == "EXPAND"
        ]

        if not cores_for_expand:
            return

        _header("EXPAND PHASE")
        _status("EXPAND", f"{len(cores_for_expand)} core(s) to expand.")

        # Build slot overrides from the core definitions.
        from pipeline.adaptive_scheduler import generate_expand_batch
        for core_id, core in cores_for_expand:
            template_id = core.get("template_id", "")
            generate_expand_batch(
                pipeline_core_id=core_id,
                pipeline_template_id=template_id,
                dataset_id=self.dataset_id,
                data_type="GROUP",
                template_doc=str(self._session_dir / "templates"),
                output_dir=str(expand_batch),
                slot_overrides_file="",
                settings_grid_file="",
                dry_run=False,
            )

        # Run backtest on expand batches.
        self._run_backtest(expand_batch, expand_result)

        _status("EXPAND", "Expand backtest complete.")

    # ── analysis & mutation ─────────────────────────────────────────────

    def _analyze(self, result_dir: Path) -> list[dict]:
        """Load probe results and run LLM analysis."""
        from pipeline.adaptive_scheduler import load_probe_results, aggregate_by_core

        results = load_probe_results(result_dir)
        if not results:
            return []

        core_stats = aggregate_by_core(results)

        diagnoses: list[dict] = []
        for core_id, stats in sorted(core_stats.items(),
                                      key=lambda x: -x[1]["sharpe_mean"]):
            if stats.get("sharpe_mean", 0) is None or stats["sharpe_mean"] < 0.3:
                continue

            diag = analyze_core(
                core_id=core_id,
                stats=stats,
                dataset_id=self.dataset_id,
                llm=self.llm,
                quiet=self.quiet,
            )
            diagnoses.append(diag)

        return diagnoses

    def _generate_mutations(
        self, expression: str, analysis: dict, metrics: dict,
    ) -> list[dict]:
        """Generate mutations based on diagnosis and metrics."""
        mutations = self.mutation_engine.rule_based_mutate(
            expression=expression,
            diagnosis=analysis,
            sharpe=metrics.get("sharpe_mean"),
            turnover=metrics.get("turnover_mean"),
            fitness=metrics.get("fitness_mean"),
        )
        return mutations

    # ── reporting ───────────────────────────────────────────────────────

    def _write_report(self, summary: dict) -> None:
        report_path = self._session_dir / "report.json"
        report_path.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print()
        _header("AGENT SESSION COMPLETE")
        print(f"  Session:     {summary['session_id']}")
        print(f"  Dataset:     {summary['dataset_id']}")
        print(f"  Idea:        {summary['source_idea'][:80]}")
        print(f"  Rounds:      {summary['total_rounds']}")
        print(f"  Cores:       {summary['total_cores']}")
        print(f"  Mutations:   {summary['mutations_applied']}")
        print(f"  Expansions:  {summary['expansions']}")
        print(f"  Finalized:   {summary['finalized']}")
        print(f"  Abandoned:   {summary['abandons']}")
        print(f"  Report:      {report_path}")
        print()

        if summary.get("cores"):
            print(f"{'Core':<42} {'Best Sh':>7} {'Decisions':<24}")
            print("-" * 73)
            for cid, info in sorted(summary["cores"].items(),
                                     key=lambda x: -(x[1].get("best_sharpe", 0) or 0)):
                sharpe = info.get("best_sharpe", 0)
                sharpe_str = f"{sharpe:.3f}" if sharpe is not None and sharpe != float("-inf") else "?"
                dec_str = ", ".join(info.get("decisions", []))
                print(f"  {cid[:40]:<42} {sharpe_str:>7} {dec_str:<24}")


# ─── CLI ────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Autonomous factor search agent — closes the full loop.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--dataset-id", required=True, help="Dataset (e.g. option8, pv13).")
    parser.add_argument("--idea", default="", help="Financial idea in plain English.")
    parser.add_argument("--template-path", default="",
                        help="Use existing template catalog instead of generating one.")
    parser.add_argument("--iterations", type=int, default=5,
                        help="Max probe → analyze → mutate rounds.")
    parser.add_argument("--output-dir", default="agent_output",
                        help="Root directory for all agent output.")
    parser.add_argument("--api-key", default="", help="DeepSeek API key.")
    parser.add_argument("--model", default="deepseek-chat", help="LLM model.")
    parser.add_argument("--mutate-mode", choices=["rule", "both"], default="rule",
                        help="Mutation mode (rule-based or rule+LLM).")
    parser.add_argument("--quiet", action="store_true", default=False)
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    api_key = args.api_key or load_api_key()

    agent = FactorAgent(
        dataset_id=args.dataset_id,
        api_key=api_key,
        model=args.model,
        output_dir=args.output_dir,
        mutate_mode=args.mutate_mode,
        quiet=args.quiet,
    )

    has_template = bool(args.template_path)

    if not args.idea and not has_template:
        parser.error("Either --idea or --template-path is required.")

    t0 = time.time()
    summary = agent.run(
        idea=args.idea,
        max_iterations=args.iterations,
        skip_template=has_template,
        template_path=args.template_path,
    )
    elapsed = time.time() - t0
    print(f"\nTotal time: {elapsed:.1f}s")


if __name__ == "__main__":
    main()
