"""
agent/direct/agent.py — Orchestrator for autonomous factor mining on WorldQuant Brain.

Delegates to six focused collaborators:

* ``JobStore``          — job state, results, history, convergence checks
* ``PhaseManager``      — creative/improvement phase state machine, directions
* ``LLMConversation``   — message list, LLM chat, tool execution
* ``PromptBuilder``     — stateless prompt construction
* ``SubmissionEngine``  — expression submission, FIFO queue, polling, retries
* ``ResponseProcessor`` — LLM response handling, phase dispatch, convergence

Usage
─────
  python -m agent.direct.cli --dataset-id pv13 --idea "Momentum plus low volatility"
"""

import json
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from agent.config import (
    MAX_CONSECUTIVE_STALL,
    MAX_CONSECUTIVE_STALL_EXPRESSION,
    MAX_IDLE_POLLS,
    MAX_ITERATIONS,
    MAX_DISCOVERY_TURNS,
    MAX_DISCOVERY_STALL,
    MIN_SHARPE, MIN_FITNESS, MIN_TURNOVER, MAX_TURNOVER,
    POLL_INTERVAL,
    TARGET_SHARPE,
)
from agent.llm_client import LLMClient, load_api_key, _extract_json
from agent.llm_logger import log_exchange
from agent.prompts import SYSTEM_PROMPT, IDEA_DISCOVERY_SYSTEM_PROMPT

from agent.direct.job_store import JobStore
from agent.direct.phase_manager import PhaseManager
from agent.direct.llm_conversation import LLMConversation
from agent.direct.prompt_builder import PromptBuilder
from agent.direct.submission_engine import SubmissionEngine
from agent.direct.response_processor import ResponseProcessor
from agent.wq_tools import WQTools


class DirectAgent:
    """Tool-based direct agent for autonomous factor mining.

    Three-phase protocol:
        1. Research — LLM explores datasets/fields/operators via tool calls
        2. Iteration — poll → analyse → improve → resubmit loop
        3. Report — summarise results, write JSON report
    """

    def __init__(
        self,
        dataset_id: str = "",
        api_key: Optional[str] = None,
        model: str = "deepseek-chat",
        output_dir: str = "agent_output",
        **kwargs,
    ):
        self.dataset_id = dataset_id
        self.output_dir = Path(output_dir)
        self.quiet = kwargs.pop("quiet", False)
        self.max_iterations = kwargs.pop("max_iterations", MAX_ITERATIONS)
        self.target_sharpe = kwargs.pop("target_sharpe", TARGET_SHARPE)

        # Components.
        self.tools = WQTools()
        self.llm = LLMClient(api_key=api_key, model=model)

        # Session identity.
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.session_dir = self.output_dir / f"direct_{self.session_id}"
        self.session_dir.mkdir(parents=True, exist_ok=True)
        self._log_path = self.session_dir / "session.log"

        # Idea discovery state.
        self._discovered_ideas = None

        # Instantiate collaborators (order respects dependencies).
        self.job_store = JobStore(log_event=self._log_event)
        self.phase_manager = PhaseManager(
            quiet=self.quiet, log_event=self._log_event,
        )
        self.llm_conv = LLMConversation(
            llm=self.llm, tools=self.tools,
            session_dir=self.session_dir, quiet=self.quiet,
            log_event=self._log_event,
        )
        self.prompt_builder = PromptBuilder(
            tools=self.tools, dataset_id=self.dataset_id,
        )
        self.submission_engine = SubmissionEngine(
            tools=self.tools, dataset_id=self.dataset_id,
            quiet=self.quiet, log_event=self._log_event,
            job_store=self.job_store, phase_manager=self.phase_manager,
        )
        self.response_processor = ResponseProcessor(
            llm_conv=self.llm_conv, prompt_builder=self.prompt_builder,
            submission_engine=self.submission_engine,
            phase_manager=self.phase_manager, job_store=self.job_store,
            tools=self.tools, quiet=self.quiet, log_event=self._log_event,
        )

        self._log_event("session_start", dataset_id=self.dataset_id, model=model)

    # ── Public API ────────────────────────────────────────────────────────

    def run(self, idea: str = "", expression: str = "", critique: str = "", settings: dict | None = None) -> dict:
        """Main entry point.  Returns the session summary dict."""
        self.idea = idea
        self.expression = expression
        self.critique = critique
        self.user_settings = settings or {}

        # ── Login ─────────────────────────────────────────────────────
        if not self.quiet:
            print(f"[{self.session_id}] Logging in to Brain API ...")
        self._log_event("login", status="starting")
        try:
            self.tools.login()
        except RuntimeError as exc:
            print(f"[FATAL] Brain login failed: {exc}")
            self._log_event("login", status="failed", error=str(exc))
            return {"error": str(exc), "session_id": self.session_id}
        if not self.quiet:
            print(f"[{self.session_id}] Login OK")
        self._log_event("login", status="ok")

        # ── Phase 0: Idea Discovery (autonomous web search) ──────────
        if not self.idea and not self.expression:
            if not self.quiet:
                print(f"[{self.session_id}] No --idea provided. Starting autonomous idea discovery...")
            self._log_event("phase_start", phase="idea_discovery", mode="autonomous")
            self._idea_discovery_phase()

            if not self.idea:
                if self.quiet:
                    print(f"[{self.session_id}] Idea discovery failed. Re-run with --idea.")
                    return {"error": "Idea discovery failed", "session_id": self.session_id}
                print(f"\n[{self.session_id}] Idea discovery was unsuccessful.")
                print("Enter a financial idea to pursue (or press Enter to exit):")
                try:
                    user_input = input("> ").strip()
                except (EOFError, KeyboardInterrupt):
                    user_input = ""
                if user_input:
                    self.idea = user_input
                    self._log_event("idea_discovery_user_fallback", idea=self.idea[:200])
                    print(f"  Using: {self.idea[:100]}...")
                else:
                    print("No idea provided. Exiting.")
                    return {"error": "No idea provided", "session_id": self.session_id}

        # ── Build initial context ──────────────────────────────────────
        self.response_processor.original_idea = self.idea or self.expression or ""
        kb_entries = self._gather_relevant_knowledge()
        context = self.prompt_builder.build_context(
            idea=self.idea, expression=self.expression,
            critique=self.critique, user_settings=self.user_settings,
            kb_entries=kb_entries,
            discovered_ideas=getattr(self, '_discovered_ideas', None),
        )
        self.llm_conv.init_messages(SYSTEM_PROMPT, context)

        # ── Phase 1: Research ➜ initial submission ────────────────────
        if not self.quiet:
            print(f"[{self.session_id}] Phase 1 — Research & initial submission", flush=True)
        self._log_event("phase_start", phase="research", idea=idea, expression=expression)
        self._research_phase()

        if self.job_store.converged:
            return self._final_report()

        # ── Phase 2: Iteration ────────────────────────────────────────
        if not self.quiet:
            print(f"[{self.session_id}] Phase 2 — Iteration", flush=True)
        self._log_event("phase_start", phase="iteration")
        self._iteration_phase()

        # ── Phase 3: Report ───────────────────────────────────────────
        return self._final_report()

    # ── Phase 0: Idea Discovery ───────────────────────────────────────────

    def _idea_discovery_phase(self) -> None:
        """Autonomous web-based idea discovery (separate LLM conversation)."""
        if not self.quiet:
            print(f"[{self.session_id}] Phase 0 — Idea Discovery (searching the web...)")
        self._log_event("phase_start", phase="idea_discovery", dataset_id=self.dataset_id)

        sys_prompt = IDEA_DISCOVERY_SYSTEM_PROMPT
        user_parts = []
        if self.dataset_id:
            user_parts.append(
                f"Dataset specified: {self.dataset_id}. "
                "Focus your search on ideas relevant to this dataset."
            )
        user_parts.append(
            "Search the web for factor ideas. Read promising sources. "
            "Validate candidate expressions against real WQ operators and fields. "
            "When you have 1-3 solid ideas, submit via type 'discovery_complete'."
        )

        discovery_messages: list[dict] = [
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content": "\n".join(user_parts)},
        ]

        stall_count = 0
        discovery_exchange_counter = 0
        for turn in range(MAX_DISCOVERY_TURNS):
            try:
                content = self.llm.chat(discovery_messages, temperature=0.5)
            except RuntimeError as exc:
                if not self.quiet:
                    print(f"  [LLM ERR] {exc}")
                self._log_event("discovery_llm", turn=turn, error=str(exc)[:200])
                stall_count += 1
                if stall_count >= MAX_DISCOVERY_STALL:
                    break
                continue

            discovery_messages.append({"role": "assistant", "content": content})
            self._log_event("discovery_llm", turn=turn, preview=content[:300])

            discovery_exchange_counter += 1
            log_exchange(
                self.session_dir,
                exchange_id=f"discovery_{discovery_exchange_counter}",
                call_type="discovery",
                messages=discovery_messages[:-1],
                response=content,
                temperature=0.5,
                agent="direct",
                tag="discovery_llm",
            )

            try:
                parsed = _extract_json(content)
            except (ValueError, json.JSONDecodeError) as exc:
                discovery_messages.append({
                    "role": "user",
                    "content": f"Parse error: {exc}. Please respond with valid JSON per the protocol.",
                })
                stall_count += 1
                if stall_count >= MAX_DISCOVERY_STALL:
                    break
                continue

            rtype = parsed.get("type", "")

            if rtype == "tool_call":
                result = self.llm_conv.execute_tool(parsed)
                discovery_messages.append({
                    "role": "user",
                    "content": json.dumps(result, ensure_ascii=False, default=str),
                })
                stall_count = 0

            elif rtype == "discovery_complete":
                ideas = parsed.get("ideas", [])
                if ideas:
                    best = ideas[0]
                    self.idea = best.get("hypothesis", "").strip()
                    self._discovered_ideas = ideas
                    self._log_event(
                        "idea_discovery_complete",
                        count=len(ideas),
                        idea_preview=self.idea[:200],
                        sources=[i.get("source", "")[:100] for i in ideas],
                    )
                    if not self.quiet:
                        print(f"  [DISCOVERED] Idea: {self.idea[:120]}...")
                        for i, idea in enumerate(ideas):
                            src = idea.get("source", "")[:60]
                            fam = idea.get("family", "?")
                            hyp = idea.get("hypothesis", "")[:80]
                            print(f"    {i+1}. [{fam}] {hyp}...  ({src})")
                    return
                discovery_messages.append({
                    "role": "user",
                    "content": "Empty ideas list. Continue searching or use discovery_failed.",
                })
                stall_count += 1

            elif rtype == "discovery_failed":
                reasoning = parsed.get("reasoning", "")[:200]
                self._log_event("idea_discovery_failed", turn=turn, reasoning=reasoning)
                if not self.quiet:
                    print(f"  [DISCOVERY FAILED] {reasoning}")
                return

            else:
                discovery_messages.append({
                    "role": "user",
                    "content": f"Unknown type '{rtype}'. Use 'tool_call', 'discovery_complete', or 'discovery_failed'.",
                })
                stall_count += 1
                if stall_count >= MAX_DISCOVERY_STALL:
                    break

        self._log_event("idea_discovery_stalled", stall_count=stall_count)
        if not self.quiet:
            print(f"  [DISCOVERY STALLED] Max turns or stall limit reached.")

    # ── Phase 1: Research ─────────────────────────────────────────────────

    def _research_phase(self) -> None:
        """LLM-driven research loop: tool calls until submission or done."""
        if self.expression:
            if not self.quiet:
                settings_str = self.user_settings or {}
                print(
                    f"  [BASELINE] Submitting user expression as baseline: "
                    f"{self.expression[:70]}..."
                )
                if settings_str:
                    print(f"             Settings: {dict(settings_str)}")
            self._log_event("research_baseline", expression=self.expression[:100])
            self.submission_engine.submit_all([{
                "expression": self.expression,
                "settings": self.user_settings,
                "rationale": "User's original expression submitted as baseline",
            }])

        stall_limit = (
            MAX_CONSECUTIVE_STALL_EXPRESSION
            if self.expression
            else MAX_CONSECUTIVE_STALL
        )
        stall_count = 0

        while stall_count < stall_limit:
            response = self.llm_conv.chat()
            if response is None:
                stall_count += 1
                continue

            rtype = response.get("type", "")

            if rtype == "tool_call":
                result = self.llm_conv.execute_tool(response)
                self.llm_conv.append(result)
                stall_count += 1

            elif rtype == "submit":
                exprs = response.get("expressions", [])
                if exprs:
                    if len(exprs) < 3 and not self.quiet:
                        print(
                            f"  [WARN] LLM returned only {len(exprs)} expression(s) "
                            f"(expected 3). Submitting what we have."
                        )
                    self.submission_engine.submit_all(exprs)
                    return
                self.llm_conv.append("No expressions in submit. Please provide at least one.")

            elif rtype == "done":
                self.job_store.converged = True
                return

            else:
                self.llm_conv.append(
                    f"Unknown type '{rtype}'. Use 'tool_call', 'submit', or 'done'."
                )
                stall_count += 1

        # Stall guard.
        if self.expression:
            if not self.quiet:
                print(
                    f"[{self.session_id}] Research stalled — "
                    f"submitting user expression as baseline"
                )
            self._log_event("research_stall", stall_count=stall_count, action="submit_baseline")
            self.submission_engine.submit_all([{
                "expression": self.expression,
                "settings": {},
                "rationale": "User expression submitted as baseline after research stall",
            }])
            return
        else:
            self._log_event("research_stall", stall_count=stall_count, action="graceful_exit")
            if not self.quiet:
                print(f"[{self.session_id}] Research stalled — no expressions submitted")
            if not self.job_store.active_jobs:
                self.job_store.converged = True

    # ── Phase 2: Iteration ────────────────────────────────────────────────

    def _iteration_phase(self) -> None:
        """Poll → analyse → improve → resubmit loop."""
        self.phase_manager.set_initial_phase()

        idle_polls = 0
        heartbeat_after = 5
        next_heartbeat = heartbeat_after

        while not self.job_store.converged and self.job_store.iteration < self.max_iterations:
            if not self.job_store.has_active_or_queued():
                if self.submission_engine.queue_size():
                    self.submission_engine.drain_queue()
                if not self.job_store.active_jobs and not self.submission_engine.queue_size():
                    break

            # Poll.
            self.submission_engine.poll_all()

            # Collect completed / failed jobs.
            finished = self.job_store.get_finished_jobs()

            if finished:
                idle_polls = 0
                next_heartbeat = heartbeat_after
                self.job_store.iteration += 1
                if not self.quiet:
                    print(
                        f"[{self.session_id}] Iter {self.job_store.iteration}: "
                        f"{len(finished)} job(s) finished, "
                        f"{len(self.job_store.active_jobs) - len(finished)} running"
                    )
                self.submission_engine.drain_queue()
                self.response_processor.analyse_and_improve()
                self.job_store.move_finished_to_completed(finished)
            else:
                idle_polls += 1
                if idle_polls >= next_heartbeat and not self.quiet:
                    running = len(self.job_store.active_jobs)
                    elapsed = idle_polls * POLL_INTERVAL
                    print(
                        f"[{self.session_id}] Waiting... "
                        f"{running} job(s) running, "
                        f"{elapsed // 60}m elapsed"
                    )
                    next_heartbeat = idle_polls + heartbeat_after
                if idle_polls >= MAX_IDLE_POLLS:
                    self._log_event("idle_timeout", idle_polls=idle_polls)
                    if not self.quiet:
                        print(
                            f"[{self.session_id}] Idle timeout — "
                            f"no completions after {idle_polls} polls"
                        )
                    break

            if self.job_store.active_jobs:
                time.sleep(POLL_INTERVAL)

        self.job_store.move_remaining_to_completed()

    # ── Knowledge helper ──────────────────────────────────────────────────

    def _gather_relevant_knowledge(self) -> list[dict]:
        """Search the knowledge base for entries relevant to this session."""
        seen: set[int] = set()
        entries: list[dict] = []

        if self.dataset_id:
            for e in self.tools.search_knowledge(self.dataset_id):
                if e["id"] not in seen:
                    seen.add(e["id"])
                    entries.append(e)

        text = self.idea or self.expression or ""
        for word in text.split()[:8]:
            word = word.strip(",.!?;:")
            if len(word) > 3:
                for e in self.tools.search_knowledge(word):
                    if e["id"] not in seen:
                        seen.add(e["id"])
                        entries.append(e)

        return entries

    # ── Final report ──────────────────────────────────────────────────────

    def _final_report(self) -> dict:
        """Collate all results, record final insights, write report."""
        all_results = self.job_store.all_results()

        # Deduplicate by job_id.
        seen: set[str] = set()
        unique: list[dict] = []
        for j in all_results:
            jid = j.get("job_id", "")
            if jid not in seen:
                seen.add(jid)
                unique.append(j)

        entries = []
        for j in unique:
            entries.append({
                "expression": j.get("expression", "")[:200],
                "status": j.get("status", ""),
                "sharpe": j.get("metrics", {}).get("sharpe", None),
                "turnover": j.get("metrics", {}).get("turnover", None),
                "fitness": j.get("metrics", {}).get("fitness", None),
                "alpha_id": j.get("alpha_id", None),
                "round": j.get("round", 0),
            })
        entries.sort(key=lambda e: e.get("sharpe", 0) or 0, reverse=True)

        # Record best factor to knowledge base only if ALL criteria pass.
        if entries:
            best = entries[0]
            sharpe = best.get("sharpe") or 0
            fitness = best.get("fitness") or 0
            turnover = best.get("turnover") or 0
            if (sharpe >= MIN_SHARPE
                    and fitness >= MIN_FITNESS
                    and MIN_TURNOVER < turnover < MAX_TURNOVER):
                ds_tag = f"Dataset={self.dataset_id}  " if self.dataset_id else ""
                self.tools.add_knowledge(
                    topic="successful_factor",
                    insight=(
                        f"{ds_tag}Sharpe={sharpe}  "
                        f"Fitness={fitness}  "
                        f"Turnover={turnover:.3f}  "
                        f"Expression: {best['expression'][:200]}"
                    ),
                    source="agent",
                    tags=["successful_factor"],
                )

        best = entries[0] if entries else {}
        summary = {
            "session_id": self.session_id,
            "dataset_id": self.dataset_id,
            "idea": getattr(self, "idea", ""),
            "expression": getattr(self, "expression", ""),
            "critique": getattr(self, "critique", ""),
            "iterations": self.job_store.iteration,
            "converged": self.job_store.converged,
            "total_submissions": len(unique),
            "results": entries,
            "discovery_source": (
                getattr(self, '_discovered_ideas', None)[0].get("source", "")
                if hasattr(self, '_discovered_ideas') and self._discovered_ideas
                else ""
            ),
            "directions_summary": [
                {
                    "direction_id": d.get("direction_id", "?"),
                    "hypothesis": d.get("hypothesis", "")[:200],
                    "rounds": d.get("rounds", 0),
                    "best_sharpe": d.get("best_sharpe", 0),
                    "improvement_count": d.get("improvement_count", 0),
                    "abandoned": d.get("abandoned", False),
                    "created_at_round": d.get("created_at_round", 0),
                }
                for d in self.phase_manager.all_directions_history
            ],
        }

        report_path = self.session_dir / "report.json"
        report_path.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )

        best_sharpe = entries[0].get("sharpe", "N/A") if entries else "N/A"
        self._log_event(
            "session_end",
            iterations=self.job_store.iteration,
            total_submissions=len(unique),
            best_sharpe=best_sharpe,
            converged=self.job_store.converged,
        )
        if not self.quiet:
            print(f"\n[{self.session_id}] Report: {report_path}")
            print(
                f"[{self.session_id}] Best sharpe: {best_sharpe}  "
                f"({len(entries)} total submissions)"
            )

        return summary

    # ── Logging ───────────────────────────────────────────────────────────

    def _log_event(self, event_type: str, **data) -> None:
        """Append a structured JSON line to the session log."""
        record = {"t": datetime.now().isoformat(timespec="seconds"), "type": event_type}
        for k, v in data.items():
            if isinstance(v, str) and len(v) > 2000:
                record[k] = v[:2000] + "... [truncated]"
            else:
                record[k] = v
        line = json.dumps(record, ensure_ascii=False, default=str)
        with open(self._log_path, "a", encoding="utf-8") as fh:
            fh.write(line + "\n")
