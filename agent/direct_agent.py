"""
agent/direct_agent.py — Tool-based direct agent for autonomous factor mining.

Instead of templates with combinatorial slot-filling, the LLM directly
constructs FASTEXPR expressions using 12 research/exploration tools, then
submits via a structured protocol.  The agent maintains exactly 3 concurrent
simulations (Brain's limit) in an event-driven loop.

Modes
─────
  --idea "..."                      Start from scratch with a financial idea
  --expression "..."                Improve an existing expression
  --expression "..." --critique "..." Improve with targeted critique

Usage
─────
  python -m agent.direct_agent --dataset-id pv13 --idea "Momentum plus low volatility"
  python -m agent.direct_agent --dataset-id pv13 --expression "group_rank(ts_mean(returns,21),industry)" --critique "High turnover"
"""

import argparse
import json
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from agent.llm_client import LLMClient, load_api_key, _extract_json
from agent.wq_tools import WQTools


# ─── Constants ────────────────────────────────────────────────────────────

MAX_CONCURRENT = 3
POLL_INTERVAL = 30
MAX_ITERATIONS = 20
TARGET_SHARPE = 2.0
MAX_CONSECUTIVE_STALL = 20
MAX_IDLE_POLLS = 60  # 30 min without any completion → force conclusion

SYSTEM_PROMPT = """You are an expert quantitative factor researcher on the WorldQuant Brain platform. Your goal is to discover high-performing alpha factors (predictive stock trading signals).

## Available Tools

Call any tool below by responding with `{"type": "tool_call", "reasoning": "...", "tool": "<name>", "args": {...}}`.

| # | Tool | Args | Description |
|---|------|------|-------------|
| 1 | `list_datasets` | — | List all locally cached datasets with field counts |
| 2 | `list_fields` | dataset_id | List field id + description for a dataset |
| 3 | `get_field_detail` | field_id, dataset_id | Full metadata for one field |
| 4 | `list_all_operators` | — | Overview of all 50 WQ operators |
| 5 | `search_operators` | keyword | Search operators by name/summary |
| 6 | `get_operator_detail` | name | Full spec for one operator |
| 7 | `get_setting_schema` | — | All simulation settings with defaults |
| 8 | `get_setting_detail` | name | Detail for one setting parameter |
| 9 | `get_settings_guide` | — | Fetch official Brain settings documentation |
| 10 | `validate_expression` | expression, dataset_id | Check FASTEXPR syntax and field refs |
| 11 | `add_knowledge` | topic, insight, source | Save insight to persistent knowledge base |
| 12 | `search_knowledge` | keyword | Search persistent knowledge base |
| 13 | `list_knowledge_topics` | — | Overview of all KB topics |

## Response Protocol

After research, submit factors or conclude the session:

**SUBMIT** — `{"type": "submit", "reasoning": "...", "expressions": [{"expression": "...", "settings": {...}, "rationale": "..."}]}`
  - Provide 1-3 expressions. The agent validates and submits each.
  - Settings override the defaults. Omit to use defaults.
  - Up to 3 factors run concurrently on Brain.

**ANALYZE** — `{"type": "analyze", "reasoning": "...", "converged": false, "improvements": [...], "knowledge": [...]}`
  - Called after factor backtests complete.
  - `improvements`: list of `{replace_job_id, expression, settings, rationale}`.
  - `knowledge`: list of `{topic, insight}` to record in the knowledge base.
  - Set `converged: true` when done.

**DONE** — `{"type": "done", "reasoning": "...", "summary": {...}}`
  - Concludes the session.

## Expression Guidelines

- Use only real WQ operators (ts_mean, group_rank, ts_std, ts_sum, scale, etc.)
- Validate your expression before submitting it.
- Common lookback windows: 5, 10, 21, 63, 126, 252 (trading days).
- Neutralization reduces unwanted exposures: MARKET, INDUSTRY, SECTOR, SUBINDUSTRY.
- Higher decay = lower turnover = more capacity.
- Diverse factors across different families (momentum, mean-reversion, volatility, quality) are better than similar ones.

## Settings Constraints

- instrumentType: EQUITY only
- region: USA only
- universe: TOP3000, TOP2000, TOP1000, TOP500, TOP200
- delay: 0 or 1 (1 is standard)
- decay: any positive integer (trade-off: lower = responsive, higher = smooth)
- neutralization: MARKET, INDUSTRY, SECTOR, SUBINDUSTRY, NONE
- truncation: 0.00-1.00, step 0.01
- pasteurization: ON or OFF
- unitHandling: VERIFY only
- nanHandling: ON or OFF
- visualization: false only (no permission for true)

## Strategy

1. **Research** — Explore the dataset, understand available fields, find relevant operators
2. **Generate** — Construct 3 diverse, well-reasoned expressions
3. **Iterate** — Analyze backtest results, learn from failures, improve
4. **Record** — Use `add_knowledge` to save insights at any time, not just during analysis. If you discover something interesting during research, save it immediately."""


# ─── Direct Agent ─────────────────────────────────────────────────────────

class DirectAgent:
    """Tool-based direct agent for autonomous factor mining on WorldQuant Brain.

    The agent follows a three-phase protocol:

    Phase 1 — Research
        LLM explores datasets, fields, operators, and settings via tool calls.
        Continues until the LLM issues a ``type: "submit"`` response.

    Phase 2 — Iteration
        Poll active jobs every 30 s.  When any factor completes, send results
        to the LLM for analysis.  The LLM can improve and resubmit (replacing
        the completed slot) or signal convergence.

    Phase 3 — Report
        Summarise all results, record final insights to the knowledge base,
        and write a JSON report.
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

        # Session state.
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.messages: list[dict] = []
        self.active_jobs: dict[str, dict] = {}  # keyed by progress URL
        self.completed_results: list[dict] = []
        self.iteration = 0
        self.converged = False

        # Session directory.
        self.session_dir = self.output_dir / f"direct_{self.session_id}"
        self.session_dir.mkdir(parents=True, exist_ok=True)

    # ── public API ────────────────────────────────────────────────────────

    def run(self, idea: str = "", expression: str = "", critique: str = "") -> dict:
        """Main entry point.  Returns the session summary dict."""
        self.idea = idea
        self.expression = expression
        self.critique = critique

        # ── Login ─────────────────────────────────────────────────────
        if not self.quiet:
            print(f"[{self.session_id}] Logging in to Brain API ...")
        try:
            self.tools.login()
        except RuntimeError as exc:
            print(f"[FATAL] Brain login failed: {exc}")
            return {"error": str(exc), "session_id": self.session_id}
        if not self.quiet:
            print(f"[{self.session_id}] Login OK")

        # ── Build initial context ──────────────────────────────────────
        context = self._build_context()
        self.messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": context},
        ]

        # ── Phase 1: Research ➜ initial submission ────────────────────
        if not self.quiet:
            print(f"[{self.session_id}] Phase 1 — Research & initial submission")
        self._research_phase()

        if self.converged:
            return self._final_report()

        # ── Phase 2: Iteration (poll → analyse → improve) ─────────────
        if not self.quiet:
            print(f"[{self.session_id}] Phase 2 — Iteration")
        self._iteration_phase()

        # ── Phase 3: Report ───────────────────────────────────────────
        return self._final_report()

    # ── Phase 1: Research ────────────────────────────────────────────────

    def _build_context(self) -> str:
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

        if self.idea:
            parts.append(f"Financial idea: {self.idea}")
            if not self.dataset_id:
                parts.append(
                    "Choose the most suitable dataset(s) for this idea."
                )

        if self.expression:
            parts.append(f"Existing expression: {self.expression}")
            if self.critique:
                parts.append(f"Critique / improvement goal: {self.critique}")
            parts.append(
                "Analyse this expression and generate 3 improved variants. "
                "You may research fields and operators first if needed."
            )

        # Attach relevant knowledge-base entries.
        kb_entries = self._gather_relevant_knowledge()
        if kb_entries:
            parts.append(f"\nRelevant knowledge ({len(kb_entries)} entries):")
            for e in kb_entries[:8]:
                parts.append(f"  [{e['topic']}] {e['insight'][:200]}")

        return "\n".join(parts)

    def _gather_relevant_knowledge(self) -> list[dict]:
        """Search the knowledge base for entries relevant to this session."""
        seen: set[int] = set()
        entries: list[dict] = []

        # Search by dataset id (if specified).
        if self.dataset_id:
            for e in self.tools.search_knowledge(self.dataset_id):
                if e["id"] not in seen:
                    seen.add(e["id"])
                    entries.append(e)

        # Search by idea keywords.
        text = self.idea or self.expression or ""
        for word in text.split()[:8]:
            word = word.strip(",.!?;:")
            if len(word) > 3:
                for e in self.tools.search_knowledge(word):
                    if e["id"] not in seen:
                        seen.add(e["id"])
                        entries.append(e)

        return entries

    def _research_phase(self) -> None:
        """LLM-driven research loop: tool calls until submission or done."""
        stall_count = 0

        while stall_count < MAX_CONSECUTIVE_STALL:
            response = self._llm_chat()
            if response is None:
                stall_count += 1
                continue

            rtype = response.get("type", "")

            if rtype == "tool_call":
                result = self._execute_tool(response)
                self._append(result)
                stall_count += 1

            elif rtype == "submit":
                exprs = response.get("expressions", [])
                if exprs:
                    self._submit_all(exprs)
                    return
                self._append("No expressions in submit. Please provide at least one.")

            elif rtype == "done":
                self.converged = True
                return

            else:
                self._append(
                    f"Unknown type '{rtype}'. Use 'tool_call', 'submit', or 'done'."
                )
                stall_count += 1

        # Stall guard: generate simple fallback expressions.
        if not self.quiet:
            print(f"[{self.session_id}] Research stalled — using fallback expressions")
        self._fallback_submit()

    # ── Phase 2: Iteration ────────────────────────────────────────────────

    def _iteration_phase(self) -> None:
        """Poll → analyse → improve → resubmit loop."""
        idle_polls = 0

        while not self.converged and self.iteration < self.max_iterations:
            if not self.active_jobs:
                break

            # Poll.
            self._poll_all()

            # Collect completed / failed jobs.
            finished = [
                j for j in self.active_jobs.values()
                if j["status"] in ("completed", "failed")
            ]

            if finished:
                idle_polls = 0
                self.iteration += 1
                if not self.quiet:
                    print(
                        f"[{self.session_id}] Iter {self.iteration}: "
                        f"{len(finished)} job(s) finished, "
                        f"{len(self.active_jobs) - len(finished)} running"
                    )
                self._analyse_and_improve()

                # Move finished jobs into completed_results.
                done_ids = [j["job_id"] for j in finished]
                for jid in done_ids:
                    if jid in self.active_jobs:
                        self.completed_results.append(self.active_jobs.pop(jid))
            else:
                idle_polls += 1
                if idle_polls >= MAX_IDLE_POLLS:
                    if not self.quiet:
                        print(
                            f"[{self.session_id}] Idle timeout — "
                            f"no completions after {idle_polls} polls"
                        )
                    break

            if self.active_jobs:
                time.sleep(POLL_INTERVAL)

        # Mark any remaining active jobs as concluded.
        for j in list(self.active_jobs.keys()):
            self.completed_results.append(self.active_jobs.pop(j))

    def _analyse_and_improve(self) -> None:
        """Send completed results to the LLM and process its response."""
        finished = [
            j for j in self.active_jobs.values()
            if j["status"] in ("completed", "failed")
        ]

        prompt_parts = ["The following factors have completed backtesting:\n"]
        for j in finished:
            prompt_parts.append(f"Job: ...{j['job_id'][-16:]}")
            prompt_parts.append(f"Expression: {j['expression']}")
            prompt_parts.append(f"Status: {j['status']}")
            metrics = j.get("metrics", {})
            if metrics:
                prompt_parts.append(
                    f"Sharpe: {metrics.get('sharpe', '?'):<8}  "
                    f"Turnover: {metrics.get('turnover', '?'):<8}  "
                    f"Fitness: {metrics.get('fitness', '?')}"
                )
            if j.get("error"):
                prompt_parts.append(f"Error: {j['error']}")
            prompt_parts.append("")

        prompt_parts.append(
            "Analyse these results and respond with type 'analyze' or 'done'.\n"
            "\n"
            "For type 'analyze':\n"
            "  - improvements: list of {replace_job_id, expression, settings, rationale}\n"
            "  - knowledge: list of {topic, insight} entries to save\n"
            "  - Set converged: true if target sharpe is reached or no further improvements likely\n"
            "\n"
            "Guidelines:\n"
            f"  - Sharpe > {self.target_sharpe} is excellent. Record and converge.\n"
            "  - If sharpe < 0.5 with no clear path, abandon and try a different approach.\n"
            "  - Vary lookback windows, neutralization levels, and operator combinations.\n"
            "  - Record ALL learnings (both successes and failures) to the knowledge base.\n"
            f"\nActive jobs remaining: {len([j for j in self.active_jobs.values() if j['status'] == 'running'])}"
        )

        self._append("\n".join(prompt_parts))

        # Inner loop: the LLM may call tools repeatedly before reaching a decision.
        for _ in range(MAX_CONSECUTIVE_STALL):
            response = self._llm_chat(temperature=0.4)
            if response is None:
                return

            rtype = response.get("type", "")

            if rtype == "analyze":
                self._process_analysis(response)
                return
            elif rtype == "done":
                self.converged = True
                return
            elif rtype == "tool_call":
                result = self._execute_tool(response)
                self._append(result)
                continue  # loop back with tool result
            else:
                self._append(
                    f"Unknown type '{rtype}'. Expected 'analyze', 'done', or 'tool_call'."
                )
                continue

    def _process_analysis(self, response: dict) -> None:
        """Process an LLM analysis response: record knowledge, submit improvements."""
        # Save knowledge entries.
        for k in response.get("knowledge", []):
            topic = k.get("topic", "general")
            insight = k.get("insight", "")
            if insight:
                self.tools.add_knowledge(topic, insight, source="agent")

        # Mark replaced jobs.
        for imp in response.get("improvements", []):
            replace_id = imp.get("replace_job_id")
            if replace_id and replace_id in self.active_jobs:
                self.active_jobs[replace_id]["status"] = "replaced"

        # Build and submit new expressions.
        new_exprs = []
        for imp in response.get("improvements", []):
            expr = imp.get("expression", "").strip()
            if expr:
                new_exprs.append({
                    "expression": expr,
                    "settings": imp.get("settings", {}),
                    "rationale": imp.get("rationale", ""),
                })

        if new_exprs:
            self._submit_all(new_exprs)

        # Check convergence.
        if response.get("converged", False):
            running = [j for j in self.active_jobs.values() if j["status"] == "running"]
            if not running:
                self.converged = True

    # ── Submission ────────────────────────────────────────────────────────

    def _submit_all(self, expressions: list[dict]) -> None:
        """Validate and submit up to MAX_CONCURRENT expressions."""
        submitted = 0
        for ex in expressions[:MAX_CONCURRENT]:
            expr = ex.get("expression", "").strip()
            if not expr:
                continue

            validation = self.tools.validate_expression(expr, self.dataset_id)
            if not validation.get("valid"):
                errors = validation.get("errors", [])
                if not self.quiet:
                    print(f"  [INVALID] {expr[:70]}...  {errors}")
                continue

            result = self.tools.submit_factor(
                expr,
                settings=ex.get("settings", {}),
                dataset_id=self.dataset_id,
            )

            if result.status == "submitted":
                self.active_jobs[result.job_id] = {
                    "job_id": result.job_id,
                    "expression": expr,
                    "settings": ex.get("settings", {}),
                    "rationale": ex.get("rationale", ""),
                    "status": "running",
                    "round": self.iteration,
                    "submitted_at": datetime.now().isoformat(),
                    "metrics": {},
                    "error": "",
                }
                submitted += 1
                if not self.quiet:
                    print(f"  [SUBMIT] {expr[:70]}...  ->  ...{result.job_id[-16:]}")
            else:
                if not self.quiet:
                    print(f"  [FAIL]   {expr[:70]}...  {result.error[:120]}")

        if submitted == 0 and not self.quiet:
            print("  [WARN] No valid expressions submitted this round.")

    def _fallback_submit(self) -> None:
        """Fallback expressions when the LLM stalls during research."""
        fallbacks = [
            {
                "expression": "group_rank(ts_mean(returns, 21), industry)",
                "settings": {"neutralization": "INDUSTRY", "decay": 5},
                "rationale": "21-day momentum, industry neutralized",
            },
            {
                "expression": "group_rank(ts_std(returns, 21), sector)",
                "settings": {"neutralization": "SECTOR", "decay": 10},
                "rationale": "21-day volatility, sector neutralized",
            },
            {
                "expression": "ts_mean(returns, 5) - ts_mean(returns, 21)",
                "settings": {"neutralization": "MARKET", "decay": 3},
                "rationale": "Short minus long return (mean reversion), market neutral",
            },
        ]
        self._submit_all(fallbacks)

    # ── Polling ───────────────────────────────────────────────────────────

    def _poll_all(self) -> None:
        """Poll all active jobs and update status."""
        job_ids = list(self.active_jobs.keys())
        if not job_ids:
            return

        try:
            results = self.tools.poll_results(job_ids)
        except Exception as exc:
            if not self.quiet:
                print(f"  [POLL ERR] {exc}")
            return

        for r in results:
            jid = r["job_id"]
            if jid not in self.active_jobs:
                continue
            self.active_jobs[jid]["status"] = r["status"]
            if r.get("metrics"):
                self.active_jobs[jid]["metrics"] = r["metrics"]
            if r.get("error"):
                self.active_jobs[jid]["error"] = r["error"]
            if r.get("alpha_id"):
                self.active_jobs[jid]["alpha_id"] = r["alpha_id"]

    # ── LLM communication ─────────────────────────────────────────────────

    def _llm_chat(self, temperature: float = 0.3) -> Optional[dict]:
        """Send messages to the LLM and parse a JSON response."""
        try:
            content = self.llm.chat(self.messages, temperature=temperature)
            self.messages.append({"role": "assistant", "content": content})
            return _extract_json(content)
        except (ValueError, json.JSONDecodeError) as exc:
            self._append(
                f"Failed to parse your response as JSON: {exc}. "
                "Please respond with valid JSON per the protocol."
            )
            return None
        except RuntimeError as exc:
            if not self.quiet:
                print(f"  [LLM ERR] {exc}")
            return None

    def _execute_tool(self, response: dict) -> dict:
        """Execute a tool call and return the result dict."""
        tool = response.get("tool", "")
        args = response.get("args", {})

        # Build a dispatch map keyed by tool name.
        dispatch = {
            "list_datasets": lambda: self.tools.list_datasets(),
            "list_fields": lambda: self.tools.list_fields(
                self._arg(args, "dataset_id", str)
            ),
            "get_field_detail": lambda: self.tools.get_field_detail(
                self._arg(args, "field_id", str),
                self._arg(args, "dataset_id", str),
            ),
            "list_all_operators": lambda: self.tools.list_all_operators(),
            "search_operators": lambda: self.tools.search_operators(
                self._arg(args, "keyword", str)
            ),
            "get_operator_detail": lambda: self.tools.get_operator_detail(
                self._arg(args, "name", str)
            ),
            "get_setting_schema": lambda: self.tools.get_setting_schema(),
            "get_setting_detail": lambda: self.tools.get_setting_detail(
                self._arg(args, "name", str)
            ),
            "get_settings_guide": lambda: self.tools.get_settings_guide(
                refresh=self._arg(args, "refresh", bool, default=False)
            ),
            "validate_expression": lambda: self.tools.validate_expression(
                self._arg(args, "expression", str),
                dataset_id=self._arg(args, "dataset_id", str, default=""),
            ),
            "search_knowledge": lambda: self.tools.search_knowledge(
                self._arg(args, "keyword", str)
            ),
            "list_knowledge_topics": lambda: self.tools.list_knowledge_topics(),
            "add_knowledge": lambda: self.tools.add_knowledge(
                topic=self._arg(args, "topic", str),
                insight=self._arg(args, "insight", str),
                source=self._arg(args, "source", str, default="agent"),
            ),
        }

        handler = dispatch.get(tool)
        if handler is None:
            return {
                "tool": tool,
                "error": f"Unknown tool. Available: {', '.join(sorted(dispatch))}",
            }

        try:
            payload = handler()
            return {"tool": tool, "args": args, "result": payload}
        except Exception as exc:
            return {"tool": tool, "args": args, "error": str(exc)}

    @staticmethod
    def _arg(args: dict, name: str, typ, default=None):
        """Extract *name* from *args* with type coercion."""
        val = args.get(name, default)
        if val is None:
            raise ValueError(f"Missing required argument '{name}'")
        if isinstance(val, typ):
            return val
        # Attempt coercion.
        try:
            return typ(val)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Argument '{name}' should be {typ.__name__}, got {type(val).__name__}"
            ) from exc

    # ── Message helpers ───────────────────────────────────────────────────

    def _append(self, content: str | dict) -> None:
        """Append a user message to the conversation."""
        if isinstance(content, dict):
            content = json.dumps(content, ensure_ascii=False, default=str)
        # Truncate large messages to avoid context overflow.
        if len(content) > 10000:
            content = content[:10000] + "\n... [truncated]"
        self.messages.append({"role": "user", "content": content})

    # ── Final report ──────────────────────────────────────────────────────

    def _final_report(self) -> dict:
        """Collate all results, record final insights, write report."""

        # Gather all results.
        all_results = list(self.completed_results)
        for j in self.active_jobs.values():
            all_results.append(j)

        # Deduplicate by job_id.
        seen: set[str] = set()
        unique: list[dict] = []
        for j in all_results:
            jid = j.get("job_id", "")
            if jid not in seen:
                seen.add(jid)
                unique.append(j)

        # Build summary entries.
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

        # Record best factor to knowledge base.
        if entries and (entries[0].get("sharpe") or 0) > 1.0:
            best = entries[0]
            ds_tag = f"Dataset={self.dataset_id}  " if self.dataset_id else ""
            self.tools.add_knowledge(
                topic="successful_factor",
                insight=(
                    f"{ds_tag}Sharpe={best['sharpe']}  "
                    f"Expression: {best['expression'][:200]}"
                ),
                source="agent",
            )

        summary = {
            "session_id": self.session_id,
            "dataset_id": self.dataset_id,
            "idea": getattr(self, "idea", ""),
            "expression": getattr(self, "expression", ""),
            "critique": getattr(self, "critique", ""),
            "iterations": self.iteration,
            "converged": self.converged,
            "total_submissions": len(unique),
            "results": entries,
        }

        report_path = self.session_dir / "report.json"
        report_path.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )

        best_sharpe = entries[0].get("sharpe", "N/A") if entries else "N/A"
        if not self.quiet:
            print(f"\n[{self.session_id}] Report: {report_path}")
            print(
                f"[{self.session_id}] Best sharpe: {best_sharpe}  "
                f"({len(entries)} total submissions)"
            )

        return summary


# ─── CLI ──────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Direct agent for autonomous factor mining on WorldQuant Brain.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--dataset-id", default="", help="Target dataset (e.g. pv13, option8). Omit to let agent choose.")
    parser.add_argument("--idea", default="", help="Financial idea in plain English.")
    parser.add_argument("--expression", default="", help="Existing expression to improve.")
    parser.add_argument("--critique", default="", help="Targeted critique / improvement goal.")
    parser.add_argument("--iterations", type=int, default=MAX_ITERATIONS, help="Max analysis rounds.")
    parser.add_argument("--target-sharpe", type=float, default=TARGET_SHARPE, help="Convergence target.")
    parser.add_argument("--output-dir", default="agent_output", help="Output directory.")
    parser.add_argument("--model", default="deepseek-chat", help="LLM model name.")
    parser.add_argument("--quiet", action="store_true", default=False)
    return parser


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    parser = build_parser()
    args = parser.parse_args()

    if not args.idea and not args.expression:
        parser.error("Either --idea or --expression is required.")

    api_key = load_api_key()

    agent = DirectAgent(
        dataset_id=args.dataset_id,
        api_key=api_key,
        model=args.model,
        output_dir=args.output_dir,
        quiet=args.quiet,
        max_iterations=args.iterations,
        target_sharpe=args.target_sharpe,
    )

    t0 = time.time()
    summary = agent.run(
        idea=args.idea,
        expression=args.expression,
        critique=args.critique,
    )
    elapsed = time.time() - t0
    print(f"\nTotal time: {elapsed:.1f}s")
    if summary.get("error"):
        sys.exit(1)


if __name__ == "__main__":
    main()
