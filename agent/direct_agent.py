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
from agent.llm_logger import log_exchange
from agent.wq_tools import WQTools


# ─── Constants ────────────────────────────────────────────────────────────

MAX_CONCURRENT = 3
POLL_INTERVAL = 30
MAX_ITERATIONS = 1000
TARGET_SHARPE = 2.0
MAX_CONSECUTIVE_STALL = 20
MAX_CONSECUTIVE_STALL_EXPRESSION = 40  # more slack when improving an existing expression
MAX_IDLE_POLLS = 60  # 30 min without any completion → force conclusion
MAX_KNOWLEDGE_PER_SESSION = 10   # hard cap on knowledge base entries per session
MAX_KNOWLEDGE_PER_ANALYSIS = 2   # max entries per analysis round
MAX_POLL_RETRIES = 3             # transient poll errors → retry before marking failed

# FIFO submission queue (retry when concurrency-slots full).
MAX_QUEUE_RETRIES = 5           # max retry attempts per queued expression
MAX_QUEUE_SIZE = 50             # hard cap on queue length (oldest dropped when exceeded)

# Truncation limits.
MAX_MSG_CHARS = 200_000        # per-message soft limit — covers list_fields("model77") at 122K
MAX_TOTAL_CHARS = 800_000      # session total — safety buffer below 1M-token context window
TIGHT_MSG_CHARS = 50_000       # per-message hard limit when total is near the ceiling

# Convergence criteria thresholds.
MIN_SHARPE = 1.25
MIN_FITNESS = 1.0
MIN_TURNOVER = 0.01
MAX_TURNOVER = 0.70

# Phase 0: Idea Discovery constants
MAX_DISCOVERY_TURNS = 30
MAX_DISCOVERY_STALL = 5

SYSTEM_PROMPT = """You are an expert quantitative factor researcher on the WorldQuant Brain platform. Your goal is to discover high-performing alpha factors (predictive stock trading signals).

## Available Tools

Call any tool below by responding with `{"type": "tool_call", "reasoning": "...", "tool": "<name>", "args": {...}}`.

| # | Tool | Args | Description |
|---|------|------|-------------|
| 1 | `list_datasets` | — | List all locally cached datasets with field counts |
| 2 | `list_fields` | dataset_id | List all field IDs in a dataset (no descriptions) |
| 3 | `get_field_detail` | field_id, dataset_id | Full metadata for one data field |
| 4 | `get_dataset_detail` | dataset_id | Detailed dataset description, category, and stats |
| 5 | `list_all_operators` | — | Overview of all 50 WQ operators |
| 6 | `search_operators` | keyword | Search operators by name/summary |
| 7 | `get_operator_detail` | name | Full spec for one operator |
| 8 | `get_setting_schema` | — | All simulation settings with defaults |
| 9 | `get_setting_detail` | name | Detail for one setting parameter |
| 10 | `get_settings_guide` | — | Fetch official Brain settings documentation |
| 11 | `validate_expression` | expression, dataset_id | Check FASTEXPR syntax and field refs |
| 12 | `add_knowledge` | topic, insight, source, tags | Save insight to KB. tags is optional list (e.g. ["signal_direction","operator"]) |
| 13 | `search_knowledge` | keyword, tags | Search KB by keyword and/or tag filter. tags is optional list |
| 14 | `list_knowledge_topics` | — | Overview of all KB topics |
| 15 | `list_knowledge_tags` | — | List all KB tags with entry count |

## Response Protocol

After research, submit factors or conclude the session:

**SUBMIT** — `{"type": "submit", "reasoning": "...", "expressions": [{"expression": "...", "settings": {...}, "rationale": "..."}]}`
  - Provide 1-3 expressions. The agent validates and submits each.
  - Settings override the defaults. Omit to use defaults.
  - Up to 3 factors run concurrently on Brain.

**ANALYZE** — `{"type": "analyze", "reasoning": "...", "converged": false, "improvements": [...], "knowledge": [...]}`
  - Called after factor backtests complete.
  - `improvements`: list of `{replace_job_id, expression, settings, rationale}`.
  - `knowledge`: list of `{topic, insight, tags}`. tags is optional (e.g. ["signal_direction","operator"]). Available: signal_direction, operator, normalization, neutralization, universe, dataset, field_choice, construction, combination, turnover, parameter_tuning, dead_end, technical, successful_factor.
  - Set `converged: true` when done.

**DONE** — `{"type": "done", "reasoning": "...", "summary": {...}}`
  - Concludes the session.

## Expression Guidelines

- Use only real WQ operators (ts_mean, group_rank, ts_std, ts_sum, scale, etc., list_all_operators can help you find them).
- Validate your expression before submitting it.
- Common lookback windows: 5, 10, 21, 63, 126, 252 (trading days).
- Neutralization reduces unwanted exposures: MARKET, INDUSTRY, SECTOR, SUBINDUSTRY.
- Higher decay usually leads to lower turnover.
- Diverse factors across different families (momentum, mean-reversion, volatility, quality) are better than similar ones.

## Settings Constraints

- instrumentType: EQUITY only
- region: USA only
- universe: TOP3000, TOP2000, TOP1000, TOP500, TOP200, TPOSP500. Universe is a subset of region based on liquidity; smaller universes are more liquid
- delay: Delay=1 alphas trade in the morning using data from yesterday; Delay=0 alphas trade in the evening using data from today
- decay: any positive integer. Sets input data equal to a linearly decreasing weighted average of that data over the past selected number of days.
- neutralization: MARKET, INDUSTRY, SECTOR, SUBINDUSTRY, NONE. Adjust alpha weights such that they sum to zero within each group of the selected type. For example, INDUSTRY neutralization means the alpha will have zero net exposure to each industry.
- truncation: 0.00-1.00, step 0.01. Maximum daily weight of each instrument.
- pasteurization: ON or OFF. Replaces operator input values with NaN for instruments not in the universe.
- unitHandling: VERIFY only.
- nanHandling: ON or OFF.  Allows aggregation operators to output numeric values when input values are NaN for a given instrument and date. Data with lower coverage is more likely to have NaN values and more sensitive to nanhandling.
- visualization: false only (no permission for true)

## Strategy

1. **Research** — Start with `get_dataset_detail` to understand the dataset's purpose and stats. Then use `list_fields` to see available fields. For any field that looks interesting, drill down with `get_field_detail` for complete metadata.
2. **Generate** — Construct 3 diverse, well-reasoned expressions
3. **Iterate** — Analyze backtest results, learn from failures, improve. Don't stop at barely passing — the goal is Sharpe > 1.8 with high fitness.
4. **Record** — Knowledge base is for cross-session learning, NOT session logging.
   - NEVER record: single-round results, parameter tweaks (e.g. "increasing decay from 5 to 10"), field descriptions, what operators do, or generic advice.
   - ONLY record: surprising non-obvious relationships, reusable patterns that apply across multiple factors, hard-won lessons that cost multiple rounds to discover.
   - Each entry must pass this test: "Would this save a future session real time?" If not, skip it.
   - The system enforces a hard cap: max 2 entries per analysis round, max 10 per session. Use them wisely."""


IDEA_DISCOVERY_SYSTEM_PROMPT = """You are an expert quantitative finance researcher searching for actionable factor ideas. Your goal is to discover 1-3 promising alpha factor ideas that can be implemented on the WorldQuant Brain platform.

## Available Tools

Call any tool by responding with `{"type": "tool_call", "reasoning": "...", "tool": "<name>", "args": {...}}`.

| # | Tool | Args | Description |
|---|------|------|-------------|
| 1 | `web_search` | query, max_results | Search the web (DuckDuckGo) for factor ideas |
| 2 | `fetch_webpage` | url | Fetch and read a web page |
| 3 | `list_datasets` | — | List all locally cached datasets with field counts |
| 4 | `list_fields` | dataset_id | List all field IDs in a dataset |
| 5 | `get_field_detail` | field_id, dataset_id | Full metadata for one data field |
| 6 | `get_dataset_detail` | dataset_id | Detailed dataset description |
| 7 | `list_all_operators` | — | Overview of all WQ operators |
| 8 | `search_operators` | keyword | Search operators by name/summary |
| 9 | `get_operator_detail` | name | Full spec for one operator |
| 10 | `validate_expression` | expression, dataset_id | Check FASTEXPR syntax and field refs |

## Instructions

1. SEARCH for factor ideas from multiple sources. Good starting points:
   - Web search: "quantitative factor investing ideas", "stock return prediction factors", "cross-sectional anomalies"
   - arXiv papers: "factor investing site:arxiv.org", "return prediction site:arxiv.org"
   - Try varied queries to discover diverse factor families: momentum, value, quality, volatility, size, sentiment
   - If a dataset is specified, focus on ideas relevant to that dataset

2. FETCH and read the most promising results. For each source, extract:
   - Source URL or paper title
   - The financial hypothesis / factor idea (in plain English)
   - Suggested implementation (operators, lookback windows, fields)
   - Whether the idea makes financial sense

3. VALIDATE candidate expressions using validate_expression to confirm they use real WQ operators and data fields.

   IMPORTANT: Only use real WQ operators. Valid datasets include: pv1, pv13, fundamental2, fundamental6, analyst4, model16, model77, news12, news18, option8, option9, univ1, sentiment1, socialmedia8, socialmedia12.

4. SELECT the best 1-3 ideas. Each must:
   - Be financially sound (economic rationale makes sense)
   - Be implementable on WQ (uses real operators and existing data fields)
   - Include a concrete expression or expression template
   - Be diverse across different factor families

5. SUBMIT your findings via type "discovery_complete" when ready.

## Response Protocol

**DISCOVERY COMPLETE** — When you have 1-3 well-researched ideas:
```json
{
  "type": "discovery_complete",
  "reasoning": "...summary of your research process...",
  "ideas": [
    {
      "hypothesis": "Plain English description of the factor idea and why it should work",
      "source": "URL or paper title where the idea was found",
      "expression": "Suggested WQ expression (if determined)",
      "family": "momentum / value / quality / volatility / size / sentiment / other",
      "confidence": "high / medium / low",
      "rationale": "Why this idea is promising for WQ implementation"
    }
  ]
}
```

**DISCOVERY FAILED** — If after thorough searching you cannot find any viable ideas:
```json
{
  "type": "discovery_failed",
  "reasoning": "Explain what was tried and why nothing usable was found"
}
```

## Search Strategy Tips

- Try specific queries: "new factor anomalies", "machine learning factor returns", "unusual volume return prediction", "short-term reversal factor"
- arXiv search: use web_search with "site:arxiv.org" in the query
- Cross-reference: if momentum papers mention volatility, search for that too
- Read at least 3-5 different results before converging
- If a dataset is specified, focus on ideas that match that dataset's domain (e.g. options for option8/option9)"""


# ─── Direct Agent ─────────────────────────────────────────────────────────

def _is_transient_error(error: str) -> bool:
    """Return True if *error* indicates a transient network/infra issue."""
    transient_patterns = [
        "proxy", "timeout", "connection", "reset", "refused",
        "500", "502", "503", "504",
        "too many requests", "rate limit",
        "retry", "try again",
    ]
    lower = error.lower()
    return any(p in lower for p in transient_patterns)


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
        self._knowledge_count = 0
        self._total_chars = 0
        # Retry queue: jobs with transient network errors, kept for background polling.
        self._retry_queue: dict[str, dict] = {}
        self._expression_history: list[str] = []  # all submitted expressions (for plateau detection)
        self._iteration_history: list[dict] = []  # round-by-round history with directions + results
        self._dedup_notes: list[str] = []  # cross-round dedup warnings for the analysis prompt
        self._direction_rounds: dict[str, int] = {}  # direction_id → how many rounds it's been active
        # FIFO queue: expressions waiting for a concurrency slot.
        self._submission_queue: list[dict] = []
        self._queue_errors: list[dict] = []  # validation errors from queue drain, shown to LLM

        # Session directory.
        self.session_dir = self.output_dir / f"direct_{self.session_id}"
        self.session_dir.mkdir(parents=True, exist_ok=True)

        # Structured log file (JSON Lines — every event is one line).
        self._log_path = self.session_dir / "session.log"
        # Complete LLM exchange log (untruncated prompts + responses).
        self._llm_exchange_counter = 0
        self._log_event("session_start", dataset_id=self.dataset_id, model=model)

    # ── public API ────────────────────────────────────────────────────────

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
        # Only runs when neither --idea nor --expression is provided.
        if not self.idea and not self.expression:
            if not self.quiet:
                print(f"[{self.session_id}] No --idea provided. Starting autonomous idea discovery...")
            self._log_event("phase_start", phase="idea_discovery", mode="autonomous")
            self._idea_discovery_phase()

            # If discovery still failed, try interactive fallback.
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
        context = self._build_context()
        self.messages = [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": context},
        ]
        self._total_chars = len(SYSTEM_PROMPT) + len(context)

        # ── Phase 1: Research ➜ initial submission ────────────────────
        if not self.quiet:
            print(f"[{self.session_id}] Phase 1 — Research & initial submission", flush=True)
        self._log_event("phase_start", phase="research", idea=idea, expression=expression)
        self._research_phase()

        if self.converged:
            return self._final_report()

        # ── Phase 2: Iteration (poll → analyse → improve) ─────────────
        if not self.quiet:
            print(f"[{self.session_id}] Phase 2 — Iteration", flush=True)
        self._log_event("phase_start", phase="iteration")
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
            parts.append(
                "(This expression has already been submitted as a baseline — "
                "you do NOT need to submit it again.)"
            )
            if self.user_settings:
                s = dict(self.user_settings)
                parts.append(
                    f"Baseline settings: {s}"
                )
            if self.critique:
                parts.append(f"Critique / improvement goal: {self.critique}")
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
        kb_entries = self._gather_relevant_knowledge()
        if kb_entries:
            parts.append(f"\nRelevant knowledge ({len(kb_entries)} entries):")
            for e in kb_entries[:8]:
                parts.append(f"  [{e['topic']}] {e['insight'][:200]}")

        # Attach discovered ideas from Phase 0 as reference.
        discovered = getattr(self, '_discovered_ideas', None)
        if discovered:
            parts.append("\nDiscovered candidate ideas (from web research):")
            for di in discovered[:3]:
                src = di.get("source", "")[:80]
                expr = di.get("expression", "")
                parts.append(f"  - {di.get('hypothesis', '')[:200]}")
                if src:
                    parts.append(f"    Source: {src}")
                if expr:
                    parts.append(f"    Candidate: {expr}")

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

    def _idea_discovery_phase(self) -> None:
        """Phase 0: Autonomous web-based idea discovery.

        Only runs when neither --idea nor --expression is provided.
        Uses a separate LLM conversation (not self.messages) so search
        results don't pollute the main context.
        """
        if not self.quiet:
            print(f"[{self.session_id}] Phase 0 — Idea Discovery (searching the web...)")
        self._log_event("phase_start", phase="idea_discovery", dataset_id=self.dataset_id)

        # Build system prompt with dataset context.
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

            # Log complete exchange.
            discovery_exchange_counter += 1
            log_exchange(
                self.session_dir,
                exchange_id=f"discovery_{discovery_exchange_counter}",
                call_type="discovery",
                messages=discovery_messages[:-1],  # exclude the just-appended response
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
                result = self._execute_tool(parsed)
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

    def _research_phase(self) -> None:
        """LLM-driven research loop: tool calls until submission or done."""
        # Always submit the user's original expression as a baseline first,
        # so it gets backtested regardless of what the LLM decides to do.
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
            self._submit_all([{
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
                    if len(exprs) < 3 and not self.quiet:
                        print(
                            f"  [WARN] LLM returned only {len(exprs)} expression(s) "
                            f"(expected 3). Submitting what we have."
                        )
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

        # Stall guard.
        if self.expression:
            # Submit the user's expression as a baseline so iteration can
            # start from real results instead of unrelated fallbacks.
            if not self.quiet:
                print(
                    f"[{self.session_id}] Research stalled — "
                    f"submitting user expression as baseline"
                )
            self._log_event("research_stall", stall_count=stall_count, action="submit_baseline")
            baseline = [{
                "expression": self.expression,
                "settings": {},
                "rationale": "User expression submitted as baseline after research stall",
            }]
            self._submit_all(baseline)
            return
        else:
            self._log_event("research_stall", stall_count=stall_count, action="fallback")
            if not self.quiet:
                print(f"[{self.session_id}] Research stalled — using fallback expressions")
            self._fallback_submit()

    # ── Phase 2: Iteration ────────────────────────────────────────────────

    def _iteration_phase(self) -> None:
        """Poll → analyse → improve → resubmit loop."""
        idle_polls = 0
        heartbeat_after = 5  # print heartbeat after 5 idle polls (~2.5 min)
        next_heartbeat = heartbeat_after

        while not self.converged and self.iteration < self.max_iterations:
            if not self.active_jobs and not self._retry_queue:
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
                next_heartbeat = heartbeat_after
                self.iteration += 1
                if not self.quiet:
                    print(
                        f"[{self.session_id}] Iter {self.iteration}: "
                        f"{len(finished)} job(s) finished, "
                        f"{len(self.active_jobs) - len(finished)} running"
                    )
                # Drain FIFO queue before asking the LLM for new ideas.
                self._drain_queue()

                self._analyse_and_improve()

                # Move finished jobs into completed_results.
                done_ids = [j["job_id"] for j in finished]
                for jid in done_ids:
                    if jid in self.active_jobs:
                        self.completed_results.append(self.active_jobs.pop(jid))
            else:
                idle_polls += 1
                if idle_polls >= next_heartbeat and not self.quiet:
                    running = len(self.active_jobs)
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

            if self.active_jobs:
                time.sleep(POLL_INTERVAL)

        # Mark any remaining active jobs as concluded.
        for j in list(self.active_jobs.keys()):
            self.completed_results.append(self.active_jobs.pop(j))

    def _analyse_and_improve(self) -> None:
        """Send completed results to the LLM and process its response."""
        # Clear per-round state.
        self._dedup_notes.clear()
        self._queue_errors.clear()

        finished = [
            j for j in self.active_jobs.values()
            if j["status"] in ("completed", "failed")
        ]

        # ── Plateau detection ──────────────────────────────────────────
        # If the LLM has been submitting essentially the same expression
        # many times, it's stuck in a micro-tweaking loop.
        total_subs = len(self._expression_history)
        unique_subs = len(set(self._expression_history))
        plateau_warning = ""
        if total_subs >= 9 and unique_subs <= 3:
            plateau_warning = (
                "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n"
                f"!!!  PLATEAU DETECTED: {total_subs} total submissions but  \n"
                f"!!!  only {unique_subs} UNIQUE expressions. You are stuck    \n"
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

        prompt_parts = [f"The following factors have completed backtesting:\n"]
        for j in finished:
            prompt_parts.append(f"Job: ...{j['job_id'][-16:]}")
            prompt_parts.append(f"Expression: {j['expression']}")
            prompt_parts.append(f"Status: {j['status']}")
            # Show settings so the LLM can learn which parameter values work.
            settings = j.get("settings", {}) or {}
            if settings:
                parts = []
                for k, v in sorted(settings.items()):
                    if v is not None and k != "language":
                        parts.append(f"{k}={v}")
                if parts:
                    prompt_parts.append(f"  Settings: {', '.join(parts)}")
            metrics = j.get("metrics", {})
            if metrics:
                prompt_parts.append(
                    f"Sharpe: {metrics.get('sharpe', '?'):<8}  "
                    f"Turnover: {metrics.get('turnover', '?'):<8}  "
                    f"Fitness: {metrics.get('fitness', '?'):<8}  "
                    f"MeanRet: {metrics.get('mean_return', '?'):<8}  "
                    f"Drawdown: {metrics.get('drawdown', '?'):<8}  "
                    f"Margin: {metrics.get('margin', '?')}"
                )
            checks = metrics.get("checks", [])
            if checks:
                prompt_parts.append("  Checks:")
                for c in checks:
                    name = c.get("name", "?")
                    result = c.get("result", "?")
                    limit = c.get("limit")
                    value = c.get("value")
                    detail = f"  {name}: {result}"
                    if value is not None and limit is not None:
                        detail += f"  (value={value}, limit={limit})"
                    prompt_parts.append(detail)
            if j.get("error"):
                prompt_parts.append(f"Error: {j['error']}")
            prompt_parts.append("")

        # ── Dedup notes ─────────────────────────────────────────────────
        if self._dedup_notes:
            prompt_parts.append("### Duplicate Submissions Detected:")
            for note in self._dedup_notes:
                prompt_parts.append(f"  - {note}")
            prompt_parts.append("")

        # ── Queue errors (validation failures from FIFO retry) ─────────
        if self._queue_errors:
            prompt_parts.append("### Previously Queued Expression Errors:")
            prompt_parts.append(
                "The following expressions from the retry queue failed validation "
                "and need your attention. Fix the expression or abandon its direction."
            )
            for qe in self._queue_errors:
                prompt_parts.append(f"  - {qe['expression'][:80]}: {qe['error'][:200]}")
            prompt_parts.append("")
            self._queue_errors.clear()

        # ── History table ──────────────────────────────────────────────
        if self._iteration_history:
            prompt_parts.append("### Iteration History\n")
            prompt_parts.append(
                "Rnd Job          Dir  Expression(truncated)       "
                "Sharpe  Turn  Fitn  MeanR  DrawD  Margin  Settings                    Checks"
            )
            # Collect all completed results with direction info from history.
            shown: set[str] = set()
            for entry in self._iteration_history[-8:]:
                rnd = entry.get("round", "?")
                for imp in entry.get("improvements", []):
                    # Find matching result by direction_id + round in completed_results.
                    target_did = imp.get("direction_id", "")
                    for j in reversed(self.completed_results):
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
                            c_str = "all PASS" if checks and all(c.get("result") == "PASS" for c in checks) else (
                                ", ".join(c.get("name", "?") for c in checks if c.get("result") != "PASS")[:20]
                            )
                            prompt_parts.append(
                                f"{rnd:<4} {short_job:<12} {direction:<4} {expr:<35} "
                                f"{sharpe:<7} {turn:<7} {fitn:<6} {meanr:<7} {drawd:<7} {margn:<7} "
                                f"{s_str:<30} {c_str}"
                            )
            prompt_parts.append("")

        if plateau_warning:
            prompt_parts.append(plateau_warning)

        prompt_parts.append(
            "Analyse these results and respond with type 'analyze' or 'done'.\n"
            "\n"
            "For type 'analyze':\n"
            "  - directions: list of {direction_id, hypothesis, target_check, approach} — "
            "your creative hypotheses BEFORE writing expressions. Each improvement links to a direction.\n"
            "  - improvements: list of {direction_id, replace_job_id, expression, settings, rationale}\n"
            "  - knowledge: list of {topic, insight} entries to save\n"
            "  - converged: true to stop, false to continue improving\n"
            "  - abandoned: true if this factor direction is a dead end and you want to start fresh\n"
            "\n"
            "### Full analyze JSON format (MUST follow exactly):\n"
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
            "IMPORTANT: You MUST include the 'directions' array with at least 1 entry. "
            "Each improvement MUST link to a direction via direction_id.\n"
            "\n"
            "### Minimum requirements (ALL must pass):\n"
            "  - Sharpe > 1.25\n"
            "  - Fitness > 1.0\n"
            "  - 0.01 < Turnover < 0.70\n"
            "  - All other Checks show PASS\n"
            "\n"
            "### Result Diagnostics\n"
            "\n"
            "Each backtest runs automated checks. When a check FAILS, use this guide:\n"
            "\n"
            "| Check | What it means | Likely causes | Try |\n"
            "|-------|--------------|---------------|-----|\n"
            "| LOW_SHARPE | Signal doesn't predict returns consistently | Weak field choice, wrong normalization, too much noise | Use different field, add ts_rank/group_rank, extend lookback window, combine with orthogonal signal |\n"
            "| LOW_FITNESS | Risk-adjusted return is low | Signal not persistent across stocks/time | Cross-sectional ranking (group_rank), stronger neutralization, reduce noise with longer decay |\n"
            "| HIGH_TURNOVER | Too much trading, high costs | Signal too noisy, decay too short | Increase decay window, add ts_mean or ts_decay_linear smoothing, neutralize volatility |\n"
            "| CONCENTRATION | Factor bets on too few stocks | Not enough neutralization, extreme values | Apply group_rank or ts_rank, stronger neutralization (INDUSTRY→SECTOR→MARKET), truncation |\n"
            "| LOW_RETURN | Absolute returns too low | Signal too weak | Use stronger fields, combine multiple signals, check field direction (try negative) |\n"
            "| HIGH_DRAWDOWN | Large peak-to-trough loss | Signal has tail risk, too concentrated | Add ts_std_dev filter, diversify across sectors, increase neutralization |\n"
            "| LOW_PNL | Total profit too low | Combined issues above | Fix the specific failing checks above |\n"
            "\n"
            "When ALL checks PASS but Sharpe < 1.25:\n"
            "  - The signal works but is too weak — try stronger normalization (group_rank), combine with complementary signals, or use a more responsive decay (3-5)\n"
            "  - This is a signal STRENGTH problem, not a signal QUALITY problem\n"
            "\n"
            "When MULTIPLE checks fail simultaneously:\n"
            "  - Fix the most severe one first (the one with the largest gap between value and limit)\n"
            "  - Often fixing one (e.g. neutralization) fixes multiple (CONCENTRATION + LOW_FITNESS)\n"
            "\n"
            "### Settings tuning (try in combination with new directions):\n"
            "  - The SAME expression with different settings can change sharpe.\n"
            "  - Options to try:\n"
            "    - neutralization: MARKET (default), INDUSTRY, SECTOR, SUBINDUSTRY, NONE\n"
            "    - decay: higher (15, 20) = lower turnover; lower (0, 3) = more responsive\n"
            "    - delay: 0 or 1 (1 avoids look-ahead bias)\n"
            "    - truncation: 0.08 (default), higher = more concentrated\n"
            "  - Record useful settings discoveries in knowledge entries.\n"
            "\n"
            "### Decision logic:\n"
            "  - Your goal is EXCELLENCE, not passing. A sharpe=1.26 factor that \"barely passes\" is NOT converged — keep iterating.\n"
            "  - Stopping early leaves value on the table.\n"
            "  - The best session ever produced Sharpe 2.0+. Don't settle for 1.3.\n"
            "  - Vary lookback windows, neutralization levels, and operator combinations.\n"
            "  - Converge only when you have tried multiple approaches and found a robust solution.\n"
            "  - Knowledge: ONLY save surprising, non-obvious, reusable insights. SKIP parameter tweaks, single-round results, field descriptions, and generic advice."
            "\n"
            "### Abandon guidelines:\n"
            "  - DO NOT abandon after a single round. You must iterate on a direction at least 3 times before giving up.\n"
            "  - Abandon ONLY when:\n"
            "    1. You have tried 3+ different variations of this direction (different operators, fields, windows), AND\n"
            "    2. ALL of them scored below 1.0 Sharpe, AND\n"
            "    3. You have a specific, concrete new direction to try instead\n"
            "  - A Sharpe of ~1.0 means the SIGNAL EXISTS but is too weak. This is an ITERATION problem, not an ABANDON problem.\n"
            "  - The system will BLOCK your abandon if a direction has fewer than 3 rounds. Iterate first.\n"
            "\n"
            "### Note on converged override:\n"
            "  - If you set converged=true but no factor passes ALL minimum requirements, the system will override it to abandoned=true and clear current jobs to try a new direction.\n"
            "  - If you genuinely cannot find any promising direction, set done=true to end the session.\n"
            "\n"
            "### Valid operators (use ONLY these — do NOT invent names):\n"
        )
        operators = self.tools.list_all_operators()
        for op in operators:
            prompt_parts.append(f"  {op['syntax']:<35s}  {op['summary']}")
        prompt_parts.append("")

        prompt_parts.append(
            f"Active jobs remaining: {len([j for j in self.active_jobs.values() if j['status'] == 'running'])}"
        )

        self._append("\n".join(prompt_parts))
        self._log_event(
            "analysis_prompt",
            finished_jobs=len(finished),
            prompt_preview="\n".join(prompt_parts)[:500],
        )

        # Inner loop: the LLM may call tools repeatedly before reaching a decision.
        invalid_retries = 0
        MAX_INVALID_RETRIES = 3
        for _ in range(MAX_CONSECUTIVE_STALL):
            response = self._llm_chat(temperature=0.4)
            if response is None:
                return

            rtype = response.get("type", "")

            if rtype == "analyze":
                ok = self._process_analysis(response)
                if ok:
                    return  # expressions submitted, normal exit
                invalid_retries += 1
                if invalid_retries >= MAX_INVALID_RETRIES:
                    self._append(
                        "Maximum invalid retries reached. Moving on."
                    )
                    return
                # All expressions were invalid — re-prompt LLM with errors
                # so it can retry with correct operator names.
                self._append(
                    "All submitted expressions used invalid operator names. "
                    "Check the '### Valid operators' list above carefully. "
                    "Use EXACT operator names (e.g. ts_std_dev, not ts_std; "
                    "ts_regression with rettype=1 for returns, not ts_returns). "
                    "Submit at least 1-3 valid replacements."
                )
                continue
            elif rtype == "done":
                self.converged = True
                return
            elif rtype == "tool_call":
                result = self._execute_tool(response)
                self._append(result)
                continue  # loop back with tool result
            else:
                self._append(
                    f"In Phase 2 (Iteration), use type 'analyze' not '{rtype}'. "
                    f"Put your new expressions inside improvements[]. "
                    f"Use type 'submit' only in Phase 1 (Research)."
                )
                continue

    def _best_sharpe(self) -> float:
        """Return the highest sharpe across all completed results."""
        best = 0.0
        for j in self.completed_results:
            s = j.get("metrics", {}).get("sharpe", 0) or 0
            if s > best:
                best = s
        for j in self.active_jobs.values():
            if j["status"] == "completed":
                s = j.get("metrics", {}).get("sharpe", 0) or 0
                if s > best:
                    best = s
        return best

    def _meets_convergence_criteria(self) -> bool:
        """Check if ANY completed result meets all convergence criteria."""
        all_results = list(self.completed_results)
        for j in self.active_jobs.values():
            if j["status"] == "completed":
                all_results.append(j)

        for j in all_results:
            m = j.get("metrics", {})
            sharpe = m.get("sharpe", 0) or 0
            fitness = m.get("fitness", 0) or 0
            turnover = m.get("turnover", 0) or 0
            checks = m.get("checks", [])
            all_checks_pass = all(
                c.get("result") == "PASS" for c in checks
            )
            if (sharpe >= MIN_SHARPE
                    and fitness >= MIN_FITNESS
                    and MIN_TURNOVER < turnover < MAX_TURNOVER
                    and all_checks_pass):
                return True
        return False

    def _process_analysis(self, response: dict) -> bool:
        """Process an LLM analysis response: record knowledge, submit improvements.
        Returns True if expressions were submitted, False otherwise."""
        # Save knowledge entries (capped per session and per round).
        knowledge_saved = 0
        kb_round_count = 0
        for k in response.get("knowledge", []):
            if self._knowledge_count >= MAX_KNOWLEDGE_PER_SESSION:
                break
            if kb_round_count >= MAX_KNOWLEDGE_PER_ANALYSIS:
                break
            topic = k.get("topic", "general")
            insight = k.get("insight", "")
            if insight:
                self.tools.add_knowledge(topic, insight, source="agent")
                self._knowledge_count += 1
                knowledge_saved += 1
                kb_round_count += 1

        # Warn if directions[] is missing (the LLM often skips it).
        if not response.get("directions"):
            self._append(
                "WARNING: Your 'analyze' response is missing the 'directions' field. "
                "You MUST include a 'directions' array with at least 1 entry containing "
                "your creative hypothesis before listing improvements. "
                "Each improvement MUST link to a direction via direction_id. "
                "See the format example in the prompt above."
            )

        # Track direction rounds (for abandon guard).
        for imp in response.get("improvements", []):
            did = imp.get("direction_id", "")
            if did:
                self._direction_rounds[did] = self._direction_rounds.get(did, 0) + 1

        # Abandon guard: block abandon if directions have fewer than 3 rounds.
        if response.get("abandoned"):
            # Check if ALL active directions have been tried at least 3 rounds.
            active_dirs = set()
            for imp in response.get("improvements", []):
                did = imp.get("direction_id", "")
                if did:
                    active_dirs.add(did)
            # Also consider directions from previous rounds that have running jobs.
            for j in self.active_jobs.values():
                did = j.get("direction_id", "")
                if did:
                    active_dirs.add(did)
            if active_dirs and all(self._direction_rounds.get(d, 0) < 3 for d in active_dirs):
                self._append(
                    "WARNING: You tried to abandon but some directions have fewer than 3 rounds. "
                    "You MUST iterate on each direction at least 3 times before giving up. "
                    "Try different settings, operators, or field combinations. "
                    "A Sharpe of ~1.0 means the signal EXISTS — it just needs strengthening, not abandoning."
                )
                self._log_event(
                    "abandon_blocked",
                    active_dirs={d: self._direction_rounds.get(d, 0) for d in active_dirs},
                )
                response["abandoned"] = False  # prevent abandon from taking effect

        # Abandon current direction: mark jobs as abandoned but DON'T clear them.
        # Keeping them running ensures the iteration loop doesn't die if new
        # submissions get rate-limited (429) due to concurrency limits.
        if response.get("abandoned"):
            for jid in self.active_jobs:
                self.active_jobs[jid]["status"] = "abandoned_from"

            # Remove queued entries for abandoned directions.
            before = len(self._submission_queue)
            self._submission_queue = [
                e for e in self._submission_queue
                if e.get("direction_id") not in self._direction_rounds
                or self._direction_rounds.get(e.get("direction_id", ""), 0) == 0
            ]
            removed = before - len(self._submission_queue)
            if removed > 0:
                self._log_event("queue_abandon_cleared", removed=removed)
                if not self.quiet:
                    print(f"  [QUEUE CLEARED] {removed} pending expression(s) removed on abandon")

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
                    "direction_id": imp.get("direction_id", ""),
                })

        self._log_event(
            "analyze",
            knowledge_saved=knowledge_saved,
            improvements_submitted=len(new_exprs),
            converged=response.get("converged", False),
        )

        # Store iteration history (for the history table).
        self._iteration_history.append({
            "round": self.iteration,
            "directions": response.get("directions", []),
            "improvements": response.get("improvements", []),
        })

        # Check convergence BEFORE submitting improvements — once converged,
        # stop submitting to avoid creating running jobs that block the exit.
        if response.get("converged", False):
            if self._meets_convergence_criteria():
                self.converged = True
                self._log_event("converged", best_sharpe=self._best_sharpe())
                if not self.quiet:
                    print(
                        f"  [CONVERGED] Criteria met. "
                        f"Best sharpe: {self._best_sharpe():.2f}"
                    )
                return True  # converged, skip improvements
            else:
                # Override: treat as abandoned instead of converged.
                best = self._best_sharpe()
                self._log_event(
                    "converge_override",
                    reason="criteria_not_met",
                    best_sharpe=best,
                    min_sharpe=MIN_SHARPE,
                )
                if not self.quiet:
                    print(
                        f"  [CONVERGE OVERRIDE] converged=true but criteria not met "
                        f"(best sharpe={best:.2f}, but checks/fitness/turnover fail). "
                        f"Continuing as abandoned — will try a new direction."
                    )
                # Abandon current jobs so the LLM starts fresh next round.
                for jid in list(self.active_jobs.keys()):
                    self.active_jobs[jid]["status"] = "abandoned"
                    self.completed_results.append(self.active_jobs.pop(jid))
                return True  # converged override — abandoned jobs

        # Only submit improvements when NOT converging.
        submitted_count = 0
        rate_limited = 0
        if new_exprs:
            submitted_count, rate_limited = self._submit_all(new_exprs)
        elif response.get("abandoned") and not self.converged:
            # Abandoned with no replacements — end the session.
            if not self.quiet:
                print(
                    f"  [ABANDONED] No replacement expressions provided. "
                    f"Ending session. Best Sharpe: {self._best_sharpe():.2f}"
                )
            self._log_event("abandoned_no_replacements", iteration=self.iteration)
            self.converged = True
            return False

        # All improvements rejected — re-prompt LLM with errors next cycle.
        if new_exprs and submitted_count == 0:
            if rate_limited > 0:
                # Rate-limited, not an LLM mistake — skip retry loop.
                if not self.quiet:
                    print(f"  [RATE LIMITED] All {rate_limited} submissions hit rate limit")
                return True
            if len(self.active_jobs) >= MAX_CONCURRENT:
                # All concurrency slots full — wait for completions.
                if not self.quiet:
                    print(f"  [SLOTS FULL] {len(self.active_jobs)} jobs still running, will retry when slots free")
                return True
            if not self.quiet:
                print("  [NO VALID] All improvements were invalid")
            return False

        return submitted_count > 0

    # ── Submission ────────────────────────────────────────────────────────

    def _submit_all(self, expressions: list[dict]) -> tuple[int, int]:
        """Validate and submit expressions, queueing extras when slots are full.

        Unlike the old behaviour (drop when no slots), any expression that
        validates successfully but can't be submitted immediately is pushed
        to the FIFO ``_submission_queue`` and retried on each future drain.

        Returns (submitted_count, rate_limited_count)."""
        # First drain any queued expressions from previous rounds.
        self._drain_queue()

        submitted = 0
        rate_limited = 0

        for ex in expressions:
            expr = ex.get("expression", "").strip()
            if not expr:
                continue

            # Validate before enqueuing — catch invalid expressions early.
            validation = self.tools.validate_expression(expr, self.dataset_id)
            if not validation.get("valid"):
                errors = validation.get("errors", [])
                self._log_event("submit", expression=expr[:100], status="invalid", errors=errors)
                if not self.quiet:
                    print(f"  [INVALID] {expr[:70]}...  {errors}")
                continue

            # Slot available → submit immediately.
            if len(self.active_jobs) < MAX_CONCURRENT:
                result = self.tools.submit_factor(
                    expr,
                    settings=ex.get("settings", {}),
                    dataset_id=self.dataset_id,
                )

                if result.status == "submitted":
                    submitted += 1
                    self._track_submitted(expr, ex, result.job_id)
                    if not self.quiet:
                        print(f"  [SUBMIT] {expr[:70]}...  ->  ...{result.job_id[-16:]}")
                else:
                    is_rate_limit = "429" in (result.error or "")
                    if is_rate_limit:
                        rate_limited += 1
                        self._enqueue(expr, ex)
                        if not self.quiet:
                            print(f"  [RATE_LIMIT→QUEUE] {expr[:70]}...  (queued for retry)")
                    else:
                        self._log_event("submit", expression=expr[:100], status="failed", error=str(result.error)[:200])
                        if not self.quiet:
                            print(f"  [FAIL] {expr[:70]}...  {result.error[:120]}")
            else:
                # No slot — enqueue for later.
                self._enqueue(expr, ex)
                if not self.quiet:
                    print(f"  [SLOTS FULL→QUEUE] {expr[:70]}...  (queue={len(self._submission_queue)})")

        if submitted == 0 and rate_limited == 0 and not self.quiet:
            print("  [WARN] No expressions submitted (all queued or invalid).")
        return submitted, rate_limited

    def _enqueue(self, expr: str, ex: dict) -> None:
        """Push an expression to the FIFO submission queue."""
        entry = {
            "expression": expr,
            "settings": ex.get("settings", {}),
            "rationale": ex.get("rationale", ""),
            "direction_id": ex.get("direction_id", ""),
            "retry_count": 0,
            "enqueued_at": datetime.now().isoformat(),
        }
        self._submission_queue.append(entry)
        self._log_event("queue_enqueue", expression=expr[:100], queue_size=len(self._submission_queue))

        # Hard cap — drop oldest entries when exceeded.
        while len(self._submission_queue) > MAX_QUEUE_SIZE:
            dropped = self._submission_queue.pop(0)
            self._log_event("queue_dropped", expression=dropped["expression"][:100], reason="queue_full")

    def _track_submitted(self, expr: str, ex: dict, job_id: str) -> None:
        """Bookkeeping after a successful submission."""
        norm = expr.replace(" ", "")
        self._expression_history.append(norm)

        # Cross-round dedup check.
        for prev in self.completed_results:
            prev_expr = prev.get("expression", "").replace(" ", "")
            prev_round = prev.get("round", 0)
            if prev_expr == norm and prev_round != self.iteration:
                note = (
                    f"Note: same expression as round {prev_round} "
                    f"(previous result: Sharpe={prev.get('metrics', {}).get('sharpe', '?')}, "
                    f"Settings={prev.get('settings', {})})"
                )
                self._dedup_notes.append(note)
                self._log_event("dedup_note", expression=expr[:100], previous_round=prev_round)
                break

        self.active_jobs[job_id] = {
            "job_id": job_id,
            "expression": expr,
            "settings": ex.get("settings", {}),
            "rationale": ex.get("rationale", ""),
            "direction_id": ex.get("direction_id", ""),
            "status": "running",
            "round": self.iteration,
            "submitted_at": datetime.now().isoformat(),
            "metrics": {},
            "error": "",
        }
        self._log_event("submit", expression=expr[:100], status="submitted", job_id=job_id)

    def _drain_queue(self) -> int:
        """Try submitting queued expressions while concurrency slots are free.

        Iterates FIFO: each entry is either submitted (slot available),
        re-queued (429, up to ``MAX_QUEUE_RETRIES`` attempts), or
        forwarded to ``_queue_errors`` (validation failure).

        Returns the number of successfully submitted entries."""
        submitted = 0
        remaining: list[dict] = []
        idx = 0

        while idx < len(self._submission_queue):
            entry = self._submission_queue[idx]

            # No more slots — keep the rest for next drain.
            if len(self.active_jobs) >= MAX_CONCURRENT:
                remaining.extend(self._submission_queue[idx:])
                break

            expr = entry.get("expression", "").strip()
            if not expr:
                idx += 1
                continue

            # Re-validate (the dataset schema may have changed).
            validation = self.tools.validate_expression(expr, self.dataset_id)
            if not validation.get("valid"):
                errors = validation.get("errors", [])
                self._queue_errors.append({
                    "expression": expr[:150],
                    "error": str(errors),
                })
                self._log_event("queue_invalid", expression=expr[:100], errors=errors)
                if not self.quiet:
                    print(f"  [QUEUE INVALID] {expr[:70]}...  {errors}")
                idx += 1
                continue  # drop from queue — will be reported to LLM

            result = self.tools.submit_factor(
                expr,
                settings=entry.get("settings", {}),
                dataset_id=self.dataset_id,
            )

            if result.status == "submitted":
                submitted += 1
                self._track_submitted(expr, entry, result.job_id)
                self._log_event("queue_dequeue", expression=expr[:100], job_id=result.job_id)
                if not self.quiet:
                    print(f"  [QUEUE→SUBMIT] {expr[:70]}...  ->  ...{result.job_id[-16:]}")
            else:
                is_rate_limit = "429" in (result.error or "")
                if is_rate_limit:
                    entry["retry_count"] += 1
                    if entry["retry_count"] >= MAX_QUEUE_RETRIES:
                        self._log_event("queue_dropped", expression=expr[:100], retries=entry["retry_count"])
                        if not self.quiet:
                            print(f"  [QUEUE DROPPED] {expr[:70]}...  max retries ({entry['retry_count']})")
                        idx += 1
                        continue  # drop
                    remaining.append(entry)
                    if not self.quiet:
                        print(f"  [QUEUE RETRY] {expr[:70]}...  (attempt {entry['retry_count']}/{MAX_QUEUE_RETRIES})")
                else:
                    # Non-rate-limit error — drop silently (unusual for a queued entry).
                    self._log_event("queue_dropped", expression=expr[:100], error=str(result.error)[:100])
                    if not self.quiet:
                        print(f"  [QUEUE FAIL] {expr[:70]}...  {result.error[:120]}")

            idx += 1

        self._submission_queue = remaining
        return submitted

    def _fallback_submit(self) -> None:
        """Fallback expressions when the LLM stalls during research."""
        fallbacks = [
            {
                "expression": "group_rank(ts_zscore(returns, 21), subindustry)",
                "settings": {"neutralization": "SUBINDUSTRY", "decay": 5},
                "rationale": "21-day risk-adjusted momentum, subindustry neutral",
            },
            {
                "expression": "ts_decay_linear(ts_scale(est_cashflow_op,252),22)-ts_decay_linear(ts_scale(est_capex,252),22)",
                "settings": {"neutralization": "INDUSTRY", "decay": 10},
                "rationale": "Cash flow quality spread — known to produce positive Sharpe",
            },
            {
                "expression": "group_rank(ts_mean(returns, 5) - ts_mean(returns, 21), subindustry)",
                "settings": {"neutralization": "SUBINDUSTRY", "decay": 3},
                "rationale": "Short-term mean reversion, subindustry neutral",
            },
        ]
        self._log_event(
            "fallback_submit",
            expressions=[fb["expression"][:100] for fb in fallbacks],
        )
        self._submit_all(fallbacks)

    # ── Polling ───────────────────────────────────────────────────────────

    def _poll_all(self) -> None:
        """Poll all active jobs and retry queue, with retry for transient errors."""
        # Poll both active jobs and network-error retry queue.
        all_ids = list(self.active_jobs.keys()) + list(self._retry_queue.keys())
        if not all_ids:
            return

        try:
            results = self.tools.poll_results(all_ids)
        except Exception as exc:
            self._log_event("poll", jobs=len(all_ids), status="error", error=str(exc)[:200])
            if not self.quiet:
                print(f"  [POLL ERR] {exc}")
            return

        statuses: dict[str, str] = {}
        recovered: list[str] = []

        for r in results:
            jid = r["job_id"]
            new_status = r["status"]

            # ── Retry queue jobs ────────────────────────────────────────
            if jid in self._retry_queue:
                job = self._retry_queue[jid]
                if new_status == "completed":
                    # Recovered! Move back to active_jobs.
                    job["status"] = "completed"
                    if r.get("metrics"):
                        job["metrics"] = r["metrics"]
                    if r.get("alpha_id"):
                        job["alpha_id"] = r["alpha_id"]
                    job.pop("error", None)
                    self.active_jobs[jid] = job
                    del self._retry_queue[jid]
                    recovered.append(jid)
                    if not self.quiet:
                        print(f"  [RECOVERED] ...{jid[-16:]}  sharpe={job.get('metrics',{}).get('sharpe','?')}")
                elif new_status == "failed" and not _is_transient_error(r.get("error", "")):
                    # Real (non-transient) failure — conclude the job.
                    job["status"] = "failed"
                    job["error"] = r.get("error", "")
                    self.completed_results.append(job)
                    del self._retry_queue[jid]
                    if not self.quiet:
                        print(f"  [FAILED]  ...{jid[-16:]}  {r['error'][:80]}")
                # else still transient → stays in retry queue, poll again next time.
                statuses[jid[-16:]] = job["status"]
                continue

            # ── Active jobs ─────────────────────────────────────────────
            if jid not in self.active_jobs:
                continue

            job = self.active_jobs[jid]

            # Transient error → retry up to MAX_POLL_RETRIES, then move to retry queue.
            if new_status == "failed" and r.get("error"):
                retries = job.get("_poll_retries", 0) + 1
                job["_poll_retries"] = retries
                if _is_transient_error(r["error"]):
                    if retries < MAX_POLL_RETRIES:
                        new_status = "running"
                        if not self.quiet:
                            print(f"  [POLL RETRY] ...{jid[-16:]}  ({retries}/{MAX_POLL_RETRIES})")
                    else:
                        # Exhausted retries → move to retry queue for background polling.
                        job["status"] = "network_error"
                        job["error"] = r["error"]
                        self._retry_queue[jid] = job
                        del self.active_jobs[jid]
                        if not self.quiet:
                            print(f"  [NETWORK ERROR] ...{jid[-16:]}  moved to retry queue")
                        statuses[jid[-16:]] = "network_error"
                        continue
                else:
                    # Non-transient failure — mark as failed permanently.
                    pass  # fall through to set status = "failed"

            job["status"] = new_status
            statuses[jid[-16:]] = new_status

            if new_status == "completed":
                if r.get("metrics"):
                    job["metrics"] = r["metrics"]
                if r.get("alpha_id"):
                    job["alpha_id"] = r["alpha_id"]
                job.pop("_poll_retries", None)
                job.pop("error", None)
            elif new_status == "failed":
                if r.get("error"):
                    job["error"] = r["error"]

        running = sum(1 for s in statuses.values() if s in ("running", "network_error"))
        completed = sum(1 for s in statuses.values() if s == "completed")
        failed = sum(1 for s in statuses.values() if s == "failed")
        self._log_event(
            "poll",
            jobs=len(all_ids),
            running=running,
            completed=completed,
            failed=failed,
            retry_queue=len(self._retry_queue),
            recovered=len(recovered),
        )

    # ── LLM communication ─────────────────────────────────────────────────

    def _llm_chat(self, temperature: float = 0.3) -> Optional[dict]:
        """Send messages to the LLM and parse a JSON response."""
        # Log full exchange to dedicated file.
        self._llm_exchange_counter += 1
        exchange_id = f"llm_{self._llm_exchange_counter}"
        messages_snapshot = list(self.messages)

        # Log last user message preview.
        last_user = None
        for m in reversed(messages_snapshot):
            if m["role"] == "user":
                raw = m["content"]
                last_user = raw[:500] if isinstance(raw, str) else str(raw)[:500]
                break
        self._log_event(
            "llm_call",
            exchange_id=exchange_id,
            temperature=temperature,
            last_user=last_user,
        )

        try:
            content = self.llm.chat(self.messages, temperature=temperature)
            self.messages.append({"role": "assistant", "content": content})
            self._total_chars += len(content)
            parsed = _extract_json(content)
            rtype = parsed.get("type", "?") if parsed else "parse_failed"

            # Log complete exchange.
            log_exchange(
                self.session_dir,
                exchange_id=exchange_id,
                call_type="analysis",
                messages=messages_snapshot,
                response=content,
                temperature=temperature,
                agent="direct",
                tag=rtype,
            )

            self._log_event(
                "llm_response", exchange_id=exchange_id, type=rtype, preview=content[:2000]
            )
            return parsed
        except (ValueError, json.JSONDecodeError) as exc:
            self._log_event(
                "llm_response", exchange_id=exchange_id, type="parse_error", error=str(exc)[:200]
            )
            self._append(
                f"Failed to parse your response as JSON: {exc}. "
                "Please respond with valid JSON per the protocol."
            )
            # Log error exchange too.
            log_exchange(
                self.session_dir,
                exchange_id=exchange_id,
                call_type="analysis",
                messages=messages_snapshot,
                response=f"[ParseError] {exc}",
                temperature=temperature,
                agent="direct",
                tag="parse_error",
            )
            return None
        except RuntimeError as exc:
            if not self.quiet:
                print(f"  [LLM ERR] {exc}")
            self._log_event(
                "llm_response", exchange_id=exchange_id, type="runtime_error", error=str(exc)[:200]
            )
            log_exchange(
                self.session_dir,
                exchange_id=exchange_id,
                call_type="analysis",
                messages=messages_snapshot,
                response=f"[RuntimeError] {exc}",
                temperature=temperature,
                agent="direct",
                tag="runtime_error",
            )
            return None
        except Exception as exc:
            # Catch network errors (ChunkedEncodingError, ConnectionError, etc.)
            # that escape the LLM client's retry loop.
            if not self.quiet:
                print(f"  [LLM NET ERR] {exc}")
            self._log_event(
                "llm_response", exchange_id=exchange_id, type="network_error", error=str(exc)[:200]
            )
            log_exchange(
                self.session_dir,
                exchange_id=exchange_id,
                call_type="analysis",
                messages=messages_snapshot,
                response=f"[NetworkError] {exc}",
                temperature=temperature,
                agent="direct",
                tag="network_error",
            )
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
            "get_dataset_detail": lambda: self.tools.get_dataset_detail(
                self._arg(args, "dataset_id", str)
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
                keyword=self._arg(args, "keyword", str, default=""),
                tags=args.get("tags"),
            ),
            "list_knowledge_topics": lambda: self.tools.list_knowledge_topics(),
            "list_knowledge_tags": lambda: self.tools.list_knowledge_tags(),
            "add_knowledge": lambda: self.tools.add_knowledge(
                topic=self._arg(args, "topic", str),
                insight=self._arg(args, "insight", str),
                source=self._arg(args, "source", str, default="agent"),
                tags=args.get("tags"),
            ),
            "web_search": lambda: self.tools.web_search(
                query=self._arg(args, "query", str),
                max_results=self._arg(args, "max_results", int, default=10),
            ),
            "fetch_webpage": lambda: self.tools.fetch_webpage(
                url=self._arg(args, "url", str),
                max_chars=self._arg(args, "max_chars", int, default=8000),
            ),
        }

        handler = dispatch.get(tool)
        if handler is None:
            err = f"Unknown tool. Available: {', '.join(sorted(dispatch))}"
            self._log_event("tool_call", tool=tool, args=args, status="unknown_tool")
            return {"tool": tool, "error": err}

        try:
            payload = handler()
            self._log_event("tool_call", tool=tool, args=args, status="ok")
            return {"tool": tool, "args": args, "result": payload}
        except Exception as exc:
            self._log_event(
                "tool_call", tool=tool, args=args, status="error", error=str(exc)[:200]
            )
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

        # Per-message truncation: apply tighter limit when total approaches the ceiling.
        limit = TIGHT_MSG_CHARS if self._total_chars > MAX_TOTAL_CHARS else MAX_MSG_CHARS
        if len(content) > limit:
            head = limit * 2 // 3
            tail = limit - head - 20
            content = content[:head] + "\n...(truncated)...\n" + content[-tail:]

        self.messages.append({"role": "user", "content": content})
        self._total_chars += len(content)

    # ── Logging ───────────────────────────────────────────────────────────

    def _log_event(self, event_type: str, **data) -> None:
        """Append a structured JSON line to the session log."""
        record = {"t": datetime.now().isoformat(timespec="seconds"), "type": event_type}
        # Truncate large string values so the log stays readable.
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
            "discovery_source": (
                getattr(self, '_discovered_ideas', None)[0].get("source", "")
                if hasattr(self, '_discovered_ideas') and self._discovered_ideas
                else ""
            ),
            "directions_summary": [
                {
                    "direction_id": d.get("direction_id", "?"),
                    "hypothesis": d.get("hypothesis", "")[:200],
                    "round": entry.get("round", 0),
                    "result_count": len([
                        imp for imp in entry.get("improvements", [])
                        if imp.get("direction_id") == d.get("direction_id")
                    ]),
                }
                for entry in self._iteration_history
                for d in entry.get("directions", [])
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
            iterations=self.iteration,
            total_submissions=len(unique),
            best_sharpe=best_sharpe,
            converged=self.converged,
        )
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
    parser.add_argument("--settings", default="",
                        help='JSON settings, e.g. \'{"neutralization":"INDUSTRY","decay":10}\'. '
                             'Overrides Brain API defaults for the baseline and shown as a hint to the LLM.')
    parser.add_argument("--neutralization", default="", choices=["MARKET", "INDUSTRY", "SECTOR", "SUBINDUSTRY", "NONE"],
                        help="Neutralization level for the baseline expression.")
    parser.add_argument("--decay", type=int, default=0,
                        help="Decay (half-life in days, e.g. 3/5/10) for the baseline expression. 0 = use Brain default.")
    parser.add_argument("--delay", type=int, default=0, choices=[0, 1],
                        help="Delay (0 or 1) for the baseline expression. 0 = use Brain default.")
    parser.add_argument("--truncation", type=float, default=0.0,
                        help="Truncation limit (0.00-1.00) for the baseline expression. 0 = use Brain default.")
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
        if not args.quiet:
            print("No --idea or --expression provided. Will attempt autonomous idea discovery (Phase 0).")
        # Phase 0 will handle the missing idea; proceed without error.

    api_key = load_api_key()

    user_settings = {}
    if args.settings:
        try:
            user_settings = json.loads(args.settings)
        except json.JSONDecodeError as e:
            parser.error(f"Invalid --settings JSON: {e}")

    # Individual CLI args override JSON keys (so both can be used together).
    if args.neutralization:
        user_settings["neutralization"] = args.neutralization
    if args.decay:
        user_settings["decay"] = args.decay
    if args.delay:
        user_settings["delay"] = args.delay
    if args.truncation:
        user_settings["truncation"] = args.truncation

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
    try:
        summary = agent.run(
            idea=args.idea,
            expression=args.expression,
            critique=args.critique,
            settings=user_settings,
        )
    except Exception as exc:
        print(f"\n[FATAL] Agent crashed: {exc}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        # Write a minimal report so the session isn't lost.
        summary = agent._final_report()
    elapsed = time.time() - t0
    print(f"\nTotal time: {elapsed:.1f}s")
    if summary.get("error"):
        sys.exit(1)


if __name__ == "__main__":
    main()
