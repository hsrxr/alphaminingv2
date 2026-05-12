"""Response processor — LLM response handling, phase dispatch, and convergence."""

from agent.config import (
    MAX_CONSECUTIVE_STALL,
    MAX_KNOWLEDGE_PER_SESSION,
    MAX_KNOWLEDGE_PER_ANALYSIS,
    MIN_SHARPE,
)


class ResponseProcessor:
    """Processes LLM analysis responses: phase dispatch, submission, convergence.

    This is the most协作-intensive component.  It coordinates across all
    other services:

    * ``llm_conv`` — to send/receive messages and execute tool calls
    * ``prompt_builder`` — to construct analysis prompts
    * ``submission_engine`` — to submit expressions and clear queue state
    * ``phase_manager`` — to manage directions and phase transitions
    * ``job_store`` — for convergence checks and history recording
    """

    def __init__(self, llm_conv, prompt_builder, submission_engine,
                 phase_manager, job_store, tools, quiet: bool, log_event):
        self.llm_conv = llm_conv
        self.prompt_builder = prompt_builder
        self.submission_engine = submission_engine
        self.phase_manager = phase_manager
        self.job_store = job_store
        self.tools = tools
        self.quiet = quiet
        self._log_event = log_event

        self._knowledge_count = 0
        self.original_idea: str = ""

    # ── Main analysis loop ─────────────────────────────────────────────────

    def analyse_and_improve(self) -> None:
        """Send completed results to the LLM and process its response."""
        # Clear per-round state (NOT validation_warnings — those are cleared
        # after display in _build_common_prompt_sections).
        self.submission_engine.clear_per_round_state()

        finished = self.job_store.get_finished_jobs()

        # Gather data from all services for prompt construction.
        total_subs, unique_subs = self.job_store.get_expression_history_stats()
        prompt_parts = self.prompt_builder.build_common_sections(
            finished,
            expression_history_total=total_subs,
            expression_history_unique=unique_subs,
            iteration_history=self.job_store._iteration_history,
            completed_results=self.job_store.completed_results,
            active_jobs_running=self.job_store.get_running_count(),
            dedup_notes=self.submission_engine._dedup_notes,
            queue_errors=self.submission_engine._queue_errors,
            validation_warnings=self.submission_engine._validation_warnings,
            phase=self.phase_manager.phase,
            directions=self.phase_manager.directions,
        )

        if self.phase_manager.phase == "creative":
            prompt_parts.extend(
                self.prompt_builder.build_creative_prompt(
                    phase_rounds=self.phase_manager.phase_rounds,
                    best_sharpe=self.job_store.best_sharpe(),
                    completed_results=self.job_store.completed_results,
                    all_directions_history=self.phase_manager.all_directions_history,
                    original_idea=self.original_idea,
                )
            )
        else:
            prompt_parts.extend(
                self.prompt_builder.build_improvement_prompt(
                    phase_rounds=self.phase_manager.phase_rounds,
                )
            )

        self.llm_conv.append("\n".join(prompt_parts))
        self._log_event(
            "analysis_prompt",
            finished_jobs=len(finished),
            phase=self.phase_manager.phase,
            prompt_preview="\n".join(prompt_parts)[:500],
        )

        # Inner loop: the LLM may call tools repeatedly before reaching a decision.
        invalid_retries = 0
        MAX_INVALID_RETRIES = 3
        for _ in range(MAX_CONSECUTIVE_STALL):
            response = self.llm_conv.chat(temperature=0.4)
            if response is None:
                return

            rtype = response.get("type", "")

            if rtype == "analyze":
                ok = self._process_analysis(response)
                if ok:
                    return  # expressions submitted, normal exit
                invalid_retries += 1
                if invalid_retries >= MAX_INVALID_RETRIES:
                    self.llm_conv.append(
                        "Maximum invalid retries reached. Moving on."
                    )
                    return
                self.llm_conv.append(
                    "All submitted expressions used invalid operator names. "
                    "Check the '### Valid operators' list above carefully. "
                    "Use EXACT operator names (e.g. ts_std_dev, not ts_std; "
                    "ts_regression with rettype=1 for returns, not ts_returns). "
                    "Submit at least 1-3 valid replacements."
                )
                continue
            elif rtype == "done":
                self.job_store.converged = True
                return
            elif rtype == "tool_call":
                result = self.llm_conv.execute_tool(response)
                self.llm_conv.append(result)
                continue
            else:
                self.llm_conv.append(
                    f"In Phase 2 (Iteration), use type 'analyze' not '{rtype}'. "
                    f"Put your new expressions inside improvements[]. "
                    f"Use type 'submit' only in Phase 1 (Research)."
                )
                continue

    # ── Phase-dispatched analysis ──────────────────────────────────────────

    def _process_analysis(self, response: dict) -> bool:
        """Route to phase-specific handler. Returns True if expressions submitted."""
        knowledge_saved = self._save_knowledge(response)

        if self.phase_manager.phase == "creative":
            ok = self._process_creative_analysis(response, knowledge_saved)
        else:
            ok = self._process_improvement_analysis(response, knowledge_saved)

        # Increment phase round counter on PhaseManager
        self.phase_manager._phase_rounds += 1

        # Check phase transition AFTER processing.
        if self.phase_manager.check_phase_transition(self.job_store.best_sharpe()):
            if not self.phase_manager.reentry_guard:
                self.phase_manager.reentry_guard = True
                self.analyse_and_improve()
                self.phase_manager.reentry_guard = False

        # Converge override → re-prompt LLM with creative context.
        if self.phase_manager.converge_override_triggered:
            self.phase_manager.converge_override_triggered = False
            if not self.phase_manager.reentry_guard:
                if not self.quiet:
                    print("  [CONVERGE OVERRIDE] Re-prompting with CREATIVE context")
                self.phase_manager.reentry_guard = True
                self.analyse_and_improve()
                self.phase_manager.reentry_guard = False

        return ok

    def _process_creative_analysis(self, response: dict, knowledge_saved: int) -> bool:
        """Creative phase: persist new directions, submit linking improvements."""
        raw_directions = response.get("directions", [])
        if not raw_directions:
            self.llm_conv.append(
                "CREATIVE PHASE ERROR: You must provide 'directions' with "
                "{direction_id, hypothesis, target_check, approach}. "
                "Please regenerate with new hypotheses."
            )
            return False

        # Persist new directions, replacing old.
        self.phase_manager.set_directions(raw_directions, self.job_store.iteration)

        # Clear queue (old directions no longer valid).
        self.submission_engine._submission_queue = []
        self.submission_engine._queue_errors.clear()

        # Switch to improvement after creative.
        self.phase_manager.switch_to_improvement(self.job_store.best_sharpe)

        # Process improvements linking to new directions.
        return self._submit_improvements(response, knowledge_saved)

    def _process_improvement_analysis(self, response: dict, knowledge_saved: int) -> bool:
        """Improvement phase: validate improvements, process abandons."""
        # ── Reject directions[] if present ──
        if response.get("directions"):
            active_ids = self.phase_manager.get_active_direction_ids()
            self.llm_conv.append(
                "IMPROVEMENT PHASE WARNING: 'directions' field ignored. "
                "You are in improvement phase — iterate on EXISTING directions only. "
                f"Active: {sorted(active_ids)}. "
                "If all directions are exhausted, the system will auto-enter CREATIVE phase."
            )

        # ── Track direction rounds ──
        for imp in response.get("improvements", []):
            did = imp.get("direction_id", "")
            if did:
                self.phase_manager.track_improvement(did)

        # ── Collect and process abandonment requests ──
        abandon_ids = self.phase_manager.collect_abandon_requests(response)
        if abandon_ids:
            approved, blocked = self.phase_manager.filter_abandon_requests(abandon_ids)
            if blocked:
                self.llm_conv.append(
                    "WARNING: Cannot abandon direction(s) "
                    f"{blocked} with fewer than 3 rounds. "
                    "Iterate more before giving up."
                )
                self._log_event("abandon_blocked", blocked=blocked)

            if approved:
                # Clear queue entries for abandoned directions.
                self.submission_engine.clear_queue_for_directions(approved)
                # Mark running jobs as abandoned_from.
                self.job_store.mark_abandoned_from(approved)

        # ── Validate direction_ids in improvements ──
        original = response.get("improvements", [])
        valid, warnings = self.phase_manager.validate_improvement_direction_ids(original)
        for w in warnings:
            self.llm_conv.append(w)

        if not valid and original:
            active_ids = self.phase_manager.get_active_direction_ids()
            self.llm_conv.append(
                "ALL improvements had invalid direction_ids. "
                "Please use one of the active direction IDs: "
                f"{sorted(active_ids)}"
            )
            return False

        response["improvements"] = valid
        return self._submit_improvements(response, knowledge_saved)

    # ── Shared submission logic ────────────────────────────────────────────

    def _submit_improvements(self, response: dict, knowledge_saved: int) -> bool:
        """Shared submission logic used by both phases."""
        # Mark replaced jobs.
        for imp in response.get("improvements", []):
            replace_id = imp.get("replace_job_id")
            self.job_store.mark_replaced(replace_id)

        # Build new expressions.
        new_exprs = []
        for imp in response.get("improvements", []):
            expr = imp.get("expression", "").strip()
            if expr:
                new_exprs.append({
                    "expression": expr,
                    "settings": imp.get("settings", {}),
                    "rationale": imp.get("rationale", ""),
                    "direction_id": imp.get("direction_id", ""),
                })

        self._log_event(
            "analyze",
            knowledge_saved=knowledge_saved,
            improvements_submitted=len(new_exprs),
            converged=response.get("converged", False),
            phase=self.phase_manager.phase,
        )

        # Store iteration history.
        entry = {
            "round": self.job_store.iteration,
            "phase": self.phase_manager.phase,
            "improvements": response.get("improvements", []),
            "active_directions": [
                {k: d[k] for k in ("direction_id", "hypothesis", "rounds", "best_sharpe", "abandoned")}
                for d in self.phase_manager.directions
            ],
        }
        if self.phase_manager.phase == "creative":
            entry["directions"] = response.get("directions", [])
        self.job_store.record_iteration(entry)

        # Check convergence BEFORE submitting.
        if response.get("converged", False):
            if self.job_store.meets_convergence_criteria():
                self.job_store.converged = True
                self._log_event("converged", best_sharpe=self.job_store.best_sharpe())
                if not self.quiet:
                    print(
                        f"  [CONVERGED] Criteria met. "
                        f"Best sharpe: {self.job_store.best_sharpe():.2f}"
                    )
                return True
            else:
                best = self.job_store.best_sharpe()
                allowed = self.phase_manager.set_converge_override()
                self._log_event(
                    "converge_override",
                    reason="criteria_not_met",
                    best_sharpe=best,
                    min_sharpe=MIN_SHARPE,
                    override_count=self.phase_manager.converge_override_count,
                    allowed=allowed,
                )
                if not allowed:
                    # Override limit reached — force-converge.
                    if not self.quiet:
                        print(
                            f"  [CONVERGE LIMIT] Override limit reached "
                            f"(best sharpe={best:.2f}). Forcing convergence."
                        )
                    self.job_store.converged = True
                    return True
                if not self.quiet:
                    print(
                        f"  [CONVERGE OVERRIDE] converged=true but criteria not met "
                        f"(best sharpe={best:.2f}). Switching to CREATIVE phase."
                    )
                self.phase_manager.switch_to_creative(self.job_store.best_sharpe)
                # Fall through — submit new_exprs so they get tested.

        # Submit improvements.
        submitted_count = 0
        rate_limited = 0
        if new_exprs:
            submitted_count, rate_limited = self.submission_engine.submit_all(new_exprs)
        elif response.get("abandoned") and not self.job_store.converged:
            if not self.quiet:
                print(
                    f"  [ABANDONED] No replacement expressions provided. "
                    f"Ending session. Best Sharpe: {self.job_store.best_sharpe():.2f}"
                )
            self._log_event("abandoned_no_replacements", iteration=self.job_store.iteration)
            self.job_store.converged = True
            return False

        if new_exprs and submitted_count == 0:
            if rate_limited > 0:
                if not self.quiet:
                    print(f"  [RATE LIMITED] All {rate_limited} submissions hit rate limit")
                return True
            if not self.quiet:
                print("  [NO VALID] All improvements were invalid")
            return False

        return submitted_count > 0

    # ── Knowledge ──────────────────────────────────────────────────────────

    def _save_knowledge(self, response: dict) -> int:
        """Save knowledge entries from an LLM response. Returns count saved."""
        saved = 0
        round_count = 0
        for k in response.get("knowledge", []):
            if self._knowledge_count >= MAX_KNOWLEDGE_PER_SESSION:
                break
            if round_count >= MAX_KNOWLEDGE_PER_ANALYSIS:
                break
            topic = k.get("topic", "general")
            insight = k.get("insight", "")
            if insight:
                self.tools.add_knowledge(topic, insight, source="agent")
                self._knowledge_count += 1
                saved += 1
                round_count += 1
        return saved
