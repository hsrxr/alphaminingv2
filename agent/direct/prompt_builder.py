"""Prompt builder — stateless construction of LLM prompts.

All data required for prompt construction is passed as parameters so the
class remains stateless and independently testable.
"""

from agent.config import (
    MIN_SHARPE, MIN_FITNESS, MIN_TURNOVER, MAX_TURNOVER,
    PHASE_MAX_ROUNDS,
)


class PromptBuilder:
    """Constructs prompt sections for the DirectAgent LLM.

    Injected with ``tools`` (for the operator list and knowledge lookups)
    and ``dataset_id``.
    """

    def __init__(self, tools, dataset_id: str):
        self.tools = tools
        self.dataset_id = dataset_id

    # ── Initial context ────────────────────────────────────────────────────

    def build_context(
        self,
        idea: str = "",
        expression: str = "",
        critique: str = "",
        user_settings: dict | None = None,
        kb_entries: list | None = None,
        discovered_ideas: list | None = None,
    ) -> str:
        """Build the initial user message based on the active mode."""
        parts: list[str] = []

        if self.dataset_id:
            parts.append(f"Dataset: {self.dataset_id}")
            parts.append(
                "Research the dataset and available operators, "
                "then generate 3 diverse factors.\n"
                "Call tools one at a time. When ready, submit via type 'submit'."
            )
        else:
            parts.append(
                "No dataset specified. Start by calling `list_datasets()` to see available datasets, "
                "then explore fields with `list_fields()` and `get_field_detail()`. "
                "You may combine fields from multiple datasets in one expression."
            )

        if idea:
            parts.append(f"Financial idea: {idea}")
            if not self.dataset_id:
                parts.append(
                    "Choose the most suitable dataset(s) for this idea."
                )

        if expression:
            parts.append(f"Existing expression: {expression}")
            parts.append(
                "(This expression has already been submitted as a baseline — "
                "you do NOT need to submit it again.)"
            )
            if user_settings:
                parts.append(f"Baseline settings: {dict(user_settings)}")
            if critique:
                parts.append(f"Critique / improvement goal: {critique}")
            parts.append(
                "Analyse this expression and generate 3 improved variants. "
                "You may research fields and operators first if needed."
            )
            parts.append(
                "HINT: The baseline uses the settings shown above (or Brain defaults). "
                "Try the same expression with DIFFERENT settings "
                "(neutralization=MARKET/INDUSTRY/SECTOR/SUBINDUSTRY, "
                "decay=1/3/5/10, delay=0/1) to explore the parameter space."
            )

        # Attach relevant knowledge-base entries.
        if kb_entries:
            parts.append(f"\nRelevant knowledge ({len(kb_entries)} entries):")
            for e in kb_entries[:8]:
                parts.append(f"  [{e['topic']}] {e['insight'][:200]}")

        # Attach discovered ideas from Phase 0 as reference.
        if discovered_ideas:
            parts.append("\nDiscovered candidate ideas (from web research):")
            for di in discovered_ideas[:3]:
                src = di.get("source", "")[:80]
                expr = di.get("expression", "")
                parts.append(f"  - {di.get('hypothesis', '')[:200]}")
                if src:
                    parts.append(f"    Source: {src}")
                if expr:
                    parts.append(f"    Candidate: {expr}")

        return "\n".join(parts)

    # ── Common sections (for the analysis prompt) ──────────────────────────

    def build_common_sections(
        self,
        finished: list[dict],
        *,
        # From JobStore
        expression_history_total: int,
        expression_history_unique: int,
        iteration_history: list[dict],
        completed_results: list[dict],
        active_jobs_running: int,
        # From SubmissionEngine
        dedup_notes: list[str],
        queue_errors: list[dict],
        validation_warnings: list[str],
        # From PhaseManager
        phase: str,
        directions: list[dict],
    ) -> list[str]:
        """Build the shared prompt sections shown every analysis round."""
        p: list[str] = []

        # ── Plateau detection ──────────────────────────────────────────
        plateau_warning = ""
        if expression_history_total >= 9 and expression_history_unique <= 3:
            plateau_warning = (
                "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n"
                f"!!!  PLATEAU DETECTED: {expression_history_total} total submissions but  \n"
                f"!!!  only {expression_history_unique} UNIQUE expressions. You are stuck    \n"
                "!!!  making tiny tweaks. STOP.                              \n"
                "!!!                                                        \n"
                "!!!  You MUST try a GENUINELY NEW direction:               \n"
                "!!!  - Call list_datasets() and explore a DIFFERENT dataset\n"
                "!!!  - Use a DIFFERENT operator family (not ts_decay_linear)\n"
                "!!!  - Try a DIFFERENT signal type (not cash-flow based)   \n"
                "!!!  - Combine your best idea with an orthogonal signal    \n"
                "!!!  - Settings tweaks alone will NOT break the plateau    \n"
                "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n"
            )

        # ── Finished job results ────────────────────────────────────────
        p.append("The following factors have completed backtesting:\n")
        for j in finished:
            p.append(f"Job: ...{j['job_id'][-16:]}")
            p.append(f"Expression: {j['expression']}")
            p.append(f"Status: {j['status']}")
            settings = j.get("settings", {}) or {}
            if settings:
                s_parts = []
                for k, v in sorted(settings.items()):
                    if v is not None and k != "language":
                        s_parts.append(f"{k}={v}")
                if s_parts:
                    p.append(f"  Settings: {', '.join(s_parts)}")
            metrics = j.get("metrics", {})
            if metrics:
                p.append(
                    f"Sharpe: {metrics.get('sharpe', '?'):<8}  "
                    f"Turnover: {metrics.get('turnover', '?'):<8}  "
                    f"Fitness: {metrics.get('fitness', '?'):<8}  "
                    f"MeanRet: {metrics.get('mean_return', '?'):<8}  "
                    f"Drawdown: {metrics.get('drawdown', '?'):<8}  "
                    f"Margin: {metrics.get('margin', '?')}"
                )
            checks = metrics.get("checks", [])
            if checks:
                p.append("  Checks:")
                for c in checks:
                    name = c.get("name", "?")
                    result = c.get("result", "?")
                    limit = c.get("limit")
                    value = c.get("value")
                    detail = f"  {name}: {result}"
                    if value is not None and limit is not None:
                        detail += f"  (value={value}, limit={limit})"
                    p.append(detail)
            if j.get("error"):
                p.append(f"Error: {j['error']}")
            p.append("")

        # ── Dedup notes ─────────────────────────────────────────────────
        if dedup_notes:
            p.append("### Duplicate Submissions Detected:")
            for note in dedup_notes:
                p.append(f"  - {note}")
            p.append("")

        # ── Queue errors ────────────────────────────────────────────────
        if queue_errors:
            p.append("### Previously Queued Expression Errors:")
            p.append(
                "The following expressions from the retry queue failed validation "
                "and need your attention. Fix the expression or abandon its direction."
            )
            for qe in queue_errors:
                p.append(f"  - {qe['expression'][:80]}: {qe['error'][:200]}")
            p.append("")

        # ── Validation warnings ─────────────────────────────────────────
        if validation_warnings:
            p.append("### Expression Warnings:")
            p.append(
                "The following non-fatal warnings were detected in your expressions. "
                "They may still backtest but could indicate suboptimal usage:"
            )
            for w in validation_warnings:
                p.append(f"  - {w}")
            p.append("")

        # ── History table ───────────────────────────────────────────────
        if iteration_history:
            p.append("### Iteration History\n")
            p.append(
                "Phs Rnd Job          Dir  Expression(truncated)      "
                "Sharpe  Turn  Fitn  MeanR  DrawD  Margin  Settings                    Checks"
            )
            shown: set[str] = set()
            for entry in iteration_history[-8:]:
                rnd = entry.get("round", "?")
                phase_char = entry.get("phase", "?")[:1].upper()
                for imp in entry.get("improvements", []):
                    target_did = imp.get("direction_id", "")
                    for j in reversed(completed_results):
                        if j.get("round") == rnd and j.get("direction_id", "") == target_did:
                            jid = j.get("job_id", "")[-12:]
                            if jid in shown:
                                continue
                            shown.add(jid)
                            m = j.get("metrics", {})
                            short_job = f"...{jid}" if j.get("job_id") else "?"
                            expr = (j.get("expression", "") or "")[:35]
                            direction = target_did or "?"
                            sharpe = f"{m.get('sharpe', 0):.2f}" if m.get('sharpe') else "?"
                            turn = f"{m.get('turnover', 0):.4f}" if m.get('turnover') else "?"
                            fitn = f"{m.get('fitness', 0):.2f}" if m.get('fitness') else "?"
                            meanr = f"{m.get('mean_return', 0):.4f}" if m.get('mean_return') else "?"
                            drawd = f"{m.get('drawdown', 0):.3f}" if m.get('drawdown') else "?"
                            margn = f"{m.get('margin', 0):.3f}" if m.get('margin') else "?"
                            settings = j.get("settings", {}) or {}
                            s_parts = []
                            for sk, sv in sorted(settings.items()):
                                if sv is not None and sk != "language":
                                    s_parts.append(f"{sk[:4]}={sv}")
                            s_str = " ".join(s_parts)[:30] if s_parts else "default"
                            checks = m.get("checks", [])
                            if checks and all(c.get("result") == "PASS" for c in checks):
                                c_str = "all PASS"
                            else:
                                c_str = ", ".join(
                                    c.get("name", "?") for c in checks if c.get("result") != "PASS"
                                )[:20]
                            p.append(
                                f"{phase_char:<4} {rnd:<4} {short_job:<12} {direction:<4} {expr:<35} "
                                f"{sharpe:<7} {turn:<7} {fitn:<6} {meanr:<7} {drawd:<7} {margn:<7} "
                                f"{s_str:<30} {c_str}"
                            )
            p.append("")

        if plateau_warning:
            p.append(plateau_warning)

        # ── Active directions status bar (improvement phase only) ─────
        if phase == "improve" and directions:
            p.append("### Active Directions\n")
            p.append(f"{'ID':<6} {'Hypothesis':<40} {'Rounds':<8} {'Best S':<8} {'Imprvs':<8} Status")
            for d in directions:
                did = d.get("direction_id", "?")
                hyp = (d.get("hypothesis", "") or "")[:38]
                rnds = d.get("rounds", 0)
                best = f"{d.get('best_sharpe', 0):.2f}" if d.get("best_sharpe") else "-"
                icnt = d.get("improvement_count", 0)
                status = "ABANDONED" if d.get("abandoned") else "active"
                p.append(f"{did:<6} {hyp:<40} {rnds:<8} {best:<8} {icnt:<8} {status}")
            p.append("")

            # Diversity enforcement.
            counts = {
                d["direction_id"]: d.get("improvement_count", 0)
                for d in directions if not d.get("abandoned")
            }
            total = sum(counts.values())
            if total >= 3:
                for did, cnt in counts.items():
                    if total > 0 and cnt / total > 0.6:
                        p.append(
                            f"NOTE: Direction {did} has {cnt}/{total} improvements "
                            f"({cnt/total:.0%}). Try to distribute more evenly across active directions.\n"
                        )
                        break

        # ── Common diagnostics & guidance ───────────────────────────────
        p.extend([
            "### Minimum requirements (ALL must pass):\n",
            "  - Sharpe > 1.25",
            "  - Fitness > 1.0",
            "  - 0.01 < Turnover < 0.70",
            "  - All other Checks show PASS",
            "",
            "### Result Diagnostics",
            "",
            "Each backtest runs automated checks. When a check FAILS, use this guide:",
            "",
            "| Check | What it means | Likely causes | Try |",
            "|-------|--------------|---------------|-----|",
            "| LOW_SHARPE | Signal doesn't predict returns consistently | Weak field choice, wrong normalization, too much noise | Use different field, add ts_rank/group_rank, extend lookback window, combine with orthogonal signal |",
            "| LOW_FITNESS | Risk-adjusted return is low | Signal not persistent across stocks/time | Cross-sectional ranking (group_rank), stronger neutralization, reduce noise with longer decay |",
            "| HIGH_TURNOVER | Too much trading, high costs | Signal too noisy, decay too short | Increase decay window, add ts_mean or ts_decay_linear smoothing, neutralize volatility |",
            "| CONCENTRATION | Factor bets on too few stocks | Not enough neutralization, extreme values | Apply group_rank or ts_rank, stronger neutralization (INDUSTRY→SECTOR→MARKET), truncation |",
            "| LOW_RETURN | Absolute returns too low | Signal too weak | Use stronger fields, combine multiple signals, check field direction (try negative) |",
            "| HIGH_DRAWDOWN | Large peak-to-trough loss | Signal has tail risk, too concentrated | Add ts_std_dev filter, diversify across sectors, increase neutralization |",
            "| LOW_PNL | Total profit too low | Combined issues above | Fix the specific failing checks above |",
            "",
            "When ALL checks PASS but Sharpe < 1.25:",
            "  - The signal works but is too weak — try stronger normalization (group_rank), combine with complementary signals, or use a more responsive decay (3-5)",
            "  - This is a signal STRENGTH problem, not a signal QUALITY problem",
            "",
            "When MULTIPLE checks fail simultaneously:",
            "  - Fix the most severe one first (the one with the largest gap between value and limit)",
            "  - Often fixing one (e.g. neutralization) fixes multiple (CONCENTRATION + LOW_FITNESS)",
            "",
            "### Settings tuning (try in combination with new directions):",
            "  - The SAME expression with different settings can change sharpe.",
            "  - Options to try:",
            "    - neutralization: MARKET (default), INDUSTRY, SECTOR, SUBINDUSTRY, NONE",
            "    - decay: higher (15, 20) = lower turnover; lower (0, 3) = more responsive",
            "    - delay: 0 or 1 (1 avoids look-ahead bias)",
            "    - truncation: 0.08 (default), higher = more concentrated",
            "  - Record useful settings discoveries in knowledge entries.",
            "",
            "### Decision logic:",
            "  - Your goal is EXCELLENCE, not passing. A sharpe=1.26 factor that \"barely passes\" is NOT converged — keep iterating.",
            "  - Stopping early leaves value on the table.",
            "  - The best session ever produced Sharpe 2.0+. Don't settle for 1.3.",
            "  - Vary lookback windows, neutralization levels, and operator combinations.",
            "  - Converge only when you have tried multiple approaches and found a robust solution.",
            "  - Knowledge: ONLY save surprising, non-obvious, reusable insights. SKIP parameter tweaks, single-round results, field descriptions, and generic advice.",
            "",
            "### Valid operators (use ONLY these — do NOT invent names):",
        ])
        operators = self.tools.list_all_operators()
        for op in operators:
            p.append(f"  {op['syntax']:<35s}  {op['summary']}")
        p.append("")

        p.append(f"Active jobs remaining: {active_jobs_running}")

        return p

    # ── Phase-specific sections ────────────────────────────────────────────

    def build_improvement_prompt(
        self,
        phase_rounds: int,
    ) -> list[str]:
        """Phase-specific sections for the improvement phase."""
        p: list[str] = []

        p.append(
            f"### Phase: IMPROVEMENT (round {phase_rounds + 1}/{PHASE_MAX_ROUNDS})\n"
            "You are in IMPROVEMENT phase. Iterate on EXISTING directions only.\n"
            "New directions are NOT allowed here — they will be rejected.\n"
        )

        p.append(
            "Analyse these results and respond with type 'analyze' or 'done'.\n"
            "\n"
            "For type 'analyze':\n"
            "  - improvements: list of {direction_id, replace_job_id, expression, settings, rationale}\n"
            "    IMPORTANT: direction_id MUST reference an active direction from the table above.\n"
            "    Unknown or abandoned direction_ids will be rejected.\n"
            "  - abandoned_directions: list of direction_ids that are dead ends (e.g. [\"d2\"])\n"
            "    Only abandon after 3+ rounds on that direction.\n"
            "  - knowledge: list of {topic, insight, tags} entries to save\n"
            "  - converged: true to stop, false to continue improving\n"
            "\n"
            "Do NOT include a 'directions' field. New directions are not allowed in this phase.\n"
            "\n"
            "### Abandon guidelines:\n"
            "  - Use abandoned_directions: [\"d2\"] to abandon INDIVIDUAL directions.\n"
            "  - DO NOT abandon a direction after a single round (minimum 3 rounds required).\n"
            "  - When ALL active directions are abandoned, the system will enter CREATIVE phase.\n"
            "  - A Sharpe of ~1.0 means the SIGNAL EXISTS but is too weak. Iterate, don't abandon.\n"
            "  - The legacy 'abandoned: true' field is treated as 'abandon ALL active directions'.\n"
            "\n"
            "### Note on converged override:\n"
            "  - If you set converged=true but no factor passes ALL minimum requirements,\n"
            "    the system will switch to CREATIVE phase for a fresh start.\n"
        )

        return p

    def build_creative_prompt(
        self,
        phase_rounds: int,
        best_sharpe: float,
        completed_results: list[dict],
        all_directions_history: list[dict],
        original_idea: str = "",
    ) -> list[str]:
        """Phase-specific sections for the creative phase."""
        p: list[str] = []

        p.append(
            "### Phase: CREATIVE — Generate Fresh Hypotheses\n"
            "You are in CREATIVE phase.\n"
        )
        if original_idea:
            p.append(
                "Remember your original financial mission:\n"
                f"  \"{original_idea[:200]}\"\n"
                "Prefer directions that explore this idea with different operators,\n"
                "settings, or normalizations. Only pivot to unrelated signals after\n"
                "genuinely exhausting the original hypothesis.\n"
            )
        p.append(
            "Based on ALL results so far, generate new directions with financial\n"
            "hypotheses. Each direction gets initial improvements.\n"
        )

        if completed_results:
            p.append("### All Completed Results Summary")
            p.append(f"Best Sharpe so far: {best_sharpe:.2f}")
            p.append(f"Total submissions: {len(completed_results)} across all directions\n")

            if all_directions_history:
                p.append("### All Directions History (past creative cycles):")
                p.append(f"{'ID':<6} {'Hypothesis':<50} {'Best S':<8} {'Rounds':<7} {'Imprvs':<7} {'Cycle':<6} Status")
                for d in all_directions_history:
                    did = d.get("direction_id", "?")
                    hyp = (d.get("hypothesis", "") or "")[:48]
                    b = f"{d.get('best_sharpe', 0):.2f}" if d.get("best_sharpe") else "-"
                    rnds = d.get("rounds", 0)
                    icnt = d.get("improvement_count", 0)
                    cycle = d.get("created_at_round", 0)
                    status = "ABANDONED" if d.get("abandoned") else "active"
                    p.append(f"{did:<6} {hyp:<50} {b:<8} {rnds:<7} {icnt:<7} {cycle:<6} {status}")
                p.append("")

        p.append(
            "Think BROADLY. Consider different datasets, operator families, and signal types.\n"
            "\n"
            "Analyse these results and respond with type 'analyze' or 'done'.\n"
            "\n"
            "For type 'analyze':\n"
            "  - directions: list of {direction_id, hypothesis, target_check, approach}\n"
            "    Provide 1 or more directions. These become active for subsequent improvement rounds.\n"
            "  - improvements: list of {direction_id, replace_job_id, expression, settings, rationale}\n"
            "    Each improvement MUST link to one of your new direction_ids.\n"
            "  - knowledge: list of {topic, insight, tags} entries to save\n"
            "  - converged: true to stop, false to continue improving\n"
            "\n"
            "### Full analyze JSON format:\n"
            "{\n"
            '  "type": "analyze",\n'
            '  "reasoning": "...",\n'
            '  "converged": false,\n'
            '  "directions": [\n'
            "    {\n"
            '      "direction_id": "d1",\n'
            '      "hypothesis": "Explain the financial intuition — WHY should this signal predict returns?",\n'
            '      "target_check": "Which check you are trying to fix (e.g. LOW_SHARPE)",\n'
            '      "approach": "What transformation to apply"\n'
            "    },\n"
            "    {\n"
            '      "direction_id": "d2",\n'
            '      "hypothesis": "A different hypothesis using different data/operators",\n'
            '      "target_check": "LOW_FITNESS",\n'
            '      "approach": "Cross-sectional ranking with longer lookback"\n'
            "    }\n"
            "  ],\n"
            '  "improvements": [\n'
            "    {\n"
            '      "direction_id": "d1",\n'
            '      "replace_job_id": "...",\n'
            '      "expression": "...",\n'
            '      "settings": {...},\n'
            '      "rationale": "..."\n'
            "    }\n"
            "  ],\n"
            '  "knowledge": [...]\n'
            "}\n"
            "IMPORTANT: You MUST include the 'directions' array with at least 1 entry.\n"
            "Each improvement MUST link to a direction via direction_id.\n"
        )

        return p
