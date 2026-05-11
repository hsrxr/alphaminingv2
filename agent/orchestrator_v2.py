"""orchestrator_v2.py — Multi-agent orchestrator for the alpha-mining system.

Coordinates the Explorer (LLM) and Optimiser (script/LLM) agents to
systematically search the WQ expression space for high-performing factors.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from agent.config import AgentConfig
from agent.explorer import Explorer
from agent.expression_fingerprint import expression_fingerprint
from agent.llm_client import LLMClient
from agent.mutation_engine import MutationEngine
from agent.param_optimizer import LLMOptimizer, OptimizerSuggestion, ParamOptimizer
from agent.results_store import ResultsStore
from agent.wq_tools import SETTINGS_SCHEMA, WQTools

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

POLL_INTERVAL = 20          # seconds between polls
MAX_CONSECUTIVE_IDLE = 60   # ~20 minutes idle → bail out
MAX_EXPLORER_ROUNDS = 8     # how many times to call the Explorer for new ideas


# ── Slot state ───────────────────────────────────────────────────────────────

@dataclass
class SlotState:
    """State of one WQ concurrent slot."""
    job_id: str = ""
    expression: str = ""
    settings: dict = field(default_factory=dict)
    status: str = "idle"      # idle | exploring | optimizing | done
    round: int = 0
    submitted_at: str = ""


# ── Orchestrator ──────────────────────────────────────────────────────────────

class OrchestratorV2:
    """Orchestrates the multi-agent factor mining pipeline.

    Usage::

        orch = OrchestratorV2(config=AgentConfig())
        report = orch.run(idea="Companies with high free cash flow outperform...")
    """

    def __init__(
        self,
        config: AgentConfig | None = None,
        output_dir: str = "agent_output",
        dataset_id: str = "",
    ):
        self.config = config or AgentConfig()
        self.dataset_id = dataset_id
        self.output_dir = Path(output_dir)
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.session_dir = self.output_dir / f"multi_{self.session_id}"
        self.session_dir.mkdir(parents=True, exist_ok=True)

        # Components.
        self.tools = WQTools()
        self.llm = LLMClient()
        self.results_store = ResultsStore(
            path=self.output_dir / "results_db.json"
        ) if self.config.reuse_results_db else ResultsStore(
            path=self.session_dir / "results.json"
        )
        self.mutation_engine = MutationEngine()

        # Agent instances.
        self.explorer = Explorer(
            tools=self.tools,
            llm=self.llm,
            results_store=self.results_store,
            mutation_engine=self.mutation_engine,
            temperature=self.config.explorer_temperature,
            retry_per_round=self.config.explorer_retry_per_round,
            log_dir=self.session_dir,
        )
        self.optimizer: ParamOptimizer | LLMOptimizer
        if self.config.optimizer_mode == "script":
            self.optimizer = ParamOptimizer(self.tools, self.results_store)
        else:
            self.optimizer = LLMOptimizer(
                self.tools, self.results_store, self.llm, log_dir=self.session_dir,
            )

        # Slot management.
        self.slots: list[SlotState] = [
            SlotState() for _ in range(3)
        ]
        self._next_explorer_round = 0
        self._optimiser_queue: list[OptimizerSuggestion] = []
        self._promising_expressions: list[str] = []  # expressions that passed screening

        # Session state.
        self.converged = False
        self.iteration = 0
        self._all_results: list[dict] = []
        self._expression_optimizers: dict[str, ParamOptimizer | LLMOptimizer] = {}

        # Logging.
        self._log_path = self.session_dir / "session.log"
        self._research_context: dict = {}

    # ── Public API ─────────────────────────────────────────────────────────

    def run(self, idea: str = "", expression: str = "") -> dict:
        """Main entry point.

        Args:
            idea: The investment thesis.
            expression: Optional initial expression (will be used as a starting point).

        Returns:
            A report dict (also written to session_dir/report.json).
        """
        # Login.
        self._log("Logging in to Brain API ...")
        try:
            self.tools.login()
        except RuntimeError as exc:
            self._log(f"[FATAL] Brain login failed: {exc}")
            return {"error": str(exc), "session_id": self.session_id}
        self._log("Login OK")

        # Load research context (dataset fields, operators).
        self._research_context = self._build_research_context()

        # If an initial expression was provided, submit it as a baseline.
        if expression:
            self._log(f"Using initial expression: {expression[:80]}...")
            self._submit_expression(expression, agent="explorer")

        # Main loop: explore → submit → poll → optimise → repeat.
        self._main_loop(idea)

        # Final report.
        return self._final_report()

    # ── Main loop ──────────────────────────────────────────────────────────

    def _main_loop(self, thesis: str) -> None:
        """Core polling-and-dispatch loop."""
        idle_count = 0

        while not self.converged:
            # 1. Poll active jobs.
            self._poll_all()

            # 2. Collect completed results.
            finished = self._collect_finished()
            if finished:
                idle_count = 0
                self.iteration += 1

                # Log results.
                for f in finished:
                    metrics = f.get("metrics", {})
                    s = metrics.get("sharpe", "?")
                    self._log(
                        f"  [{f.get('agent', '?')}] sharpe={s}  "
                        f"expr={f['expression'][:60]}..."
                    )

                # 3. Feed results back to the Explorer (it loads from ResultsStore).
                #    The Explorer automatically picks up new results via _load_history().

                # 4. Check promising expressions → add to optimiser queue.
                self._screen_promising_expressions()

            else:
                idle_count += 1
                if idle_count >= MAX_CONSECUTIVE_IDLE:
                    running = sum(1 for s in self.slots if s.status not in ("idle", "done"))
                    self._log(
                        f"Idle timeout ({idle_count} polls with no completions, "
                        f"{running} still running)."
                    )
                    break

            # 5. Dispatch: fill idle slots.
            self._dispatch(thesis)

            # 6. Convergence check.
            if self._check_convergence():
                self.converged = True
                break

            # 7. Sleep before next poll.
            if any(s.status != "idle" for s in self.slots):
                time.sleep(POLL_INTERVAL)

    # ── Dispatch ───────────────────────────────────────────────────────────

    def _dispatch(self, thesis: str) -> None:
        """Fill idle slots with the next task."""
        idle_slots = [i for i, s in enumerate(self.slots) if s.status == "idle"]
        if not idle_slots:
            return

        # Decide how to allocate idle slots.
        explorer_slots_needed = self.config.explorer_slots()
        for slot_idx in idle_slots:
            if slot_idx < explorer_slots_needed:
                # This slot is for exploration.
                if self._next_explorer_round < MAX_EXPLORER_ROUNDS:
                    self._dispatch_explorer(slot_idx, thesis)
                else:
                    # Max explorer rounds reached; use this slot for optimisation.
                    self._dispatch_optimizer(slot_idx)
            else:
                # This slot is for optimisation.
                self._dispatch_optimizer(slot_idx)

    def _dispatch_explorer(self, slot_idx: int, thesis: str) -> None:
        """Get a new candidate from the Explorer and submit it."""
        self._log(f"Slot {slot_idx}: requesting new candidate from Explorer (round {self._next_explorer_round + 1})...")

        # Build context including latest results summary.
        context = dict(self._research_context)
        context["latest_results_summary"] = self._build_results_summary()

        candidates = self.explorer.explore(
            thesis=thesis,
            context=context,
            n_candidates=1,  # one at a time
        )

        if not candidates:
            self._log(f"Slot {slot_idx}: Explorer returned no candidates.")
            self.slots[slot_idx].status = "done"
            return

        c = candidates[0]
        self._next_explorer_round += 1

        # Submit with default settings for initial screening.
        defaults = self._default_settings()
        success = self._submit_expression(
            c.expression,
            settings=defaults,
            rationale=c.rationale,
            agent="explorer",
        )

        if success:
            self._log(f"Slot {slot_idx}: submitted explorer candidate (round {self._next_explorer_round})")
        else:
            self.slots[slot_idx].status = "idle"

    def _dispatch_optimizer(self, slot_idx: int) -> None:
        """Take the next item from the optimiser queue and submit it."""
        if not self._optimiser_queue:
            # Try to generate more suggestions from a promising expression.
            if self._promising_expressions:
                expr = self._promising_expressions[0]
                self._generate_optimizer_suggestions(expr)
            else:
                self.slots[slot_idx].status = "done"
                return

        if not self._optimiser_queue:
            self.slots[slot_idx].status = "done"
            return

        suggestion = self._optimiser_queue.pop(0)
        success = self._submit_expression(
            suggestion.expression,
            settings=suggestion.settings,
            rationale=suggestion.rationale,
            agent="optimizer",
        )

        if success:
            self._log(f"Slot {slot_idx}: submitted optimizer suggestion — {suggestion.rationale[:60]}")

    # ── Submission ─────────────────────────────────────────────────────────

    def _submit_expression(
        self,
        expression: str,
        settings: dict | None = None,
        rationale: str = "",
        agent: str = "explorer",
    ) -> bool:
        """Validate and submit one expression.

        Returns True if submission succeeded, False otherwise.
        """
        # Validate.
        validation = self.tools.validate_expression(expression, self.dataset_id)
        if not validation.get("valid"):
            self._log(f"  [INVALID] {expression[:60]}...  {validation.get('errors', [])}")
            return False

        # Submit.
        result = self.tools.submit_factor(
            expression,
            settings=settings or {},
            dataset_id=self.dataset_id,
        )

        if result.status != "submitted":
            is_429 = "429" in (result.error or "")
            label = "RATE_LIMIT" if is_429 else "FAIL"
            self._log(f"  [{label}] {expression[:60]}...  {(result.error or '')[:120]}")
            return False

        # Register in an idle slot.
        for slot in self.slots:
            if slot.status == "idle":
                slot.job_id = result.job_id
                slot.expression = expression
                slot.settings = settings or {}
                slot.status = "exploring" if agent == "explorer" else "optimizing"
                slot.round = self.iteration
                slot.submitted_at = datetime.now().isoformat()
                break

        self._log(f"  [SUBMIT] {expression[:70]}...  ->  ...{result.job_id[-16:]}")
        return True

    # ── Polling ────────────────────────────────────────────────────────────

    def _poll_all(self) -> None:
        """Poll all active jobs."""
        active_ids = [
            s.job_id for s in self.slots
            if s.status in ("exploring", "optimizing") and s.job_id
        ]
        if not active_ids:
            return

        try:
            results = self.tools.poll_results(active_ids)
        except Exception as exc:
            self._log(f"  [POLL ERROR] {exc}")
            return

        for r in results:
            job_id = r.get("job_id", "")
            status = r.get("status", "")

            # Find the slot for this job.
            for slot in self.slots:
                if slot.job_id != job_id:
                    continue

                if status == "completed":
                    metrics = r.get("metrics", {})
                    alpha_id = r.get("alpha_id", "")

                    # Store in results database.
                    self.results_store.add_result(
                        expression=slot.expression,
                        settings=slot.settings,
                        sharpe=metrics.get("sharpe"),
                        turnover=metrics.get("turnover"),
                        fitness=metrics.get("fitness"),
                        alpha_id=alpha_id,
                        round_num=slot.round,
                        agent=slot.status,  # "exploring" or "optimizing"
                    )

                    # Track all results for reporting.
                    self._all_results.append({
                        "expression": slot.expression,
                        "settings": slot.settings,
                        "sharpe": metrics.get("sharpe"),
                        "turnover": metrics.get("turnover"),
                        "fitness": metrics.get("fitness"),
                        "alpha_id": alpha_id,
                        "agent": slot.status,
                        "round": slot.round,
                    })

                    slot.status = "idle"
                    slot.job_id = ""

                elif status == "failed":
                    self._log(f"  [FAILED] {slot.expression[:60]}...  {r.get('error', '')[:120]}")
                    slot.status = "idle"
                    slot.job_id = ""

                # "running" → do nothing, keep polling.
                break

    def _collect_finished(self) -> list[dict]:
        """Collect results that completed since the last poll, and reset their slots."""
        finished = [r for r in self._all_results if r.get("_collected") is None]
        for r in finished:
            r["_collected"] = True
        return finished

    # ── Screening and optimisation ─────────────────────────────────────────

    def _screen_promising_expressions(self) -> None:
        """Check if any recently completed expressions passed the screening threshold."""
        min_sharpe = self.config.min_sharpe_for_optimization

        for r in self._all_results:
            if r.get("_screened"):
                continue
            r["_screened"] = True

            sharpe = r.get("sharpe")
            if sharpe is not None and sharpe >= min_sharpe:
                expr = r["expression"]
                if expr not in self._promising_expressions:
                    self._promising_expressions.append(expr)
                    self._log(
                        f"Expression passed screening (sharpe={sharpe:.2f}): {expr[:60]}..."
                    )

    def _generate_optimizer_suggestions(self, expression: str) -> None:
        """Generate the next batch of settings suggestions for an expression."""
        # Get or create a per-expression optimiser.
        if expression not in self._expression_optimizers:
            if self.config.optimizer_mode == "script":
                opt = ParamOptimizer(self.tools, self.results_store)
            else:
                opt = LLMOptimizer(
                    self.tools, self.results_store, self.llm, log_dir=self.session_dir,
                )
            self._expression_optimizers[expression] = opt

        opt = self._expression_optimizers[expression]

        # If it's a ParamOptimizer and it's converged, move to next expression.
        if isinstance(opt, ParamOptimizer) and opt.is_converged:
            self._promising_expressions.pop(0)
            if self._promising_expressions:
                return self._generate_optimizer_suggestions(self._promising_expressions[0])
            return

        known = self.results_store.get_by_expression(expression)
        suggestions = opt.next_suggestions(expression, known_results=known)

        if suggestions:
            self._optimiser_queue.extend(suggestions)
        else:
            # No more suggestions — move on.
            if isinstance(opt, ParamOptimizer):
                if self._promising_expressions:
                    self._promising_expressions.pop(0)

    # ── Results summary (fed back to Explorer LLM) ─────────────────────────

    def _build_results_summary(self) -> str:
        """Build a concise summary of the latest results for the Explorer."""
        all_results = self.results_store.get_all()
        if not all_results:
            return "No results yet."

        # Group by expression fingerprint.
        by_fp: dict[str, list[dict]] = {}
        for r in all_results[-50:]:  # last 50
            fp = expression_fingerprint(r.get("expression", ""))
            by_fp.setdefault(fp, []).append(r)

        lines = ["Latest backtest results (expression → best sharpe):"]
        for fp, results in by_fp.items():
            best = max(results, key=lambda r: r.get("sharpe", 0) or 0)
            sharpe = best.get("sharpe", "?")
            turnover = best.get("turnover", "?")
            fitness = best.get("fitness", "?")
            expr = best.get("expression", "")[:70]
            lines.append(f"  [{fp[:6]}] sharpe={sharpe} to={turnover} fit={fitness}  {expr}")

        return "\n".join(lines)

    # ── Convergence ────────────────────────────────────────────────────────

    def _check_convergence(self) -> bool:
        """Check whether the session should converge."""
        best = self.results_store.get_best()
        if best and best.get("sharpe", 0) or 0 >= self.config.target_sharpe:
            # Found a factor meeting the target.
            self._log(f"Target sharpe reached: {best['sharpe']}")
            return True

        # No more expressions to explore and nothing to optimise.
        if (self._next_explorer_round >= MAX_EXPLORER_ROUNDS
                and not self._promising_expressions
                and not self._optimiser_queue
                and all(s.status == "idle" for s in self.slots)):
            self._log("No more expressions to explore or optimise.")
            return True

        return False

    # ── Helpers ────────────────────────────────────────────────────────────

    def _build_research_context(self) -> dict:
        """Build context with dataset, field, and operator info."""
        context: dict = {}
        try:
            datasets = self.tools.list_datasets()
            if datasets:
                context["available_datasets"] = [
                    f"{d['dataset_id']} ({d['field_count']} fields)"
                    for d in datasets[:5]
                ]

            operators = self.tools.list_all_operators()
            if operators:
                context["operators"] = [
                    f"{op.get('syntax', '')} — {op.get('summary', '')}"
                    for op in operators[:30]
                ]
        except Exception:
            pass
        return context

    def _default_settings(self) -> dict:
        """Return default simulation settings."""
        return {
            k: v["default"] for k, v in SETTINGS_SCHEMA.items()
            if v.get("_api", True) and k != "language"
        }

    def _log(self, msg: str) -> None:
        """Log a message to both stdout and the session log."""
        timestamp = datetime.now().strftime("%H:%M:%S")
        print(f"[{timestamp}] {msg}")
        self._log_event("info", message=msg)

    def _log_event(self, event_type: str, **data) -> None:
        """Append a structured JSON line to the session log."""
        record = {
            "t": datetime.now().isoformat(timespec="seconds"),
            "type": event_type,
        }
        for k, v in data.items():
            if isinstance(v, str) and len(v) > 2000:
                record[k] = v[:2000] + "... [truncated]"
            else:
                record[k] = v
        line = json.dumps(record, ensure_ascii=False, default=str)
        with open(self._log_path, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")

    # ── Final report ──────────────────────────────────────────────────────

    def _final_report(self) -> dict:
        """Collate all results and write the report."""
        entries = sorted(
            self._all_results,
            key=lambda e: e.get("sharpe", 0) or 0,
            reverse=True,
        )

        # Deduplicate.
        seen: set[str] = set()
        unique: list[dict] = []
        for e in entries:
            key = f"{e['expression']}_{json.dumps(e.get('settings', {}), sort_keys=True)}"
            if key not in seen:
                seen.add(key)
                unique.append(e)

        summary = {
            "session_id": self.session_id,
            "config": self.config.to_dict(),
            "total_submissions": len(unique),
            "explorer_rounds": self._next_explorer_round,
            "expressions_screened": len(self._promising_expressions),
            "converged": self.converged,
            "best_sharpe": unique[0].get("sharpe") if unique else None,
            "results": unique[:50],  # top 50
        }

        report_path = self.session_dir / "report.json"
        report_path.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )

        best = unique[0] if unique else None
        self._log(
            f"\nSession complete. Best sharpe: {best.get('sharpe', 'N/A') if best else 'N/A'}"
            f"  ({len(unique)} total submissions)"
        )
        self._log(f"Report: {report_path}")
        return summary
