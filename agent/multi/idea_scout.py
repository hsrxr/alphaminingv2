"""idea_scout.py — Autonomous web-based factor idea discovery.

Extracted from DirectAgent._idea_discovery_phase into a standalone module.
Uses web search + LLM to discover investable factor hypotheses.
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

from agent.llm_client import LLMClient
from agent.llm_logger import log_exchange
from agent.wq_tools import WQTools

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

MAX_DISCOVERY_TURNS = 30
MAX_DISCOVERY_STALL = 5

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

4. SELECT the best 1-3 ideas. Each must:
   - Be financially sound (economic rationale makes sense)
   - Be implementable on WQ (uses real operators and existing data fields)
   - Include a concrete expression or expression template
   - Be diverse across different factor families

5. SUBMIT your findings via type "discovery_complete" when ready.

## Response Protocol

**DISCOVERY COMPLETE**:
```json
{
  "type": "discovery_complete",
  "reasoning": "...",
  "ideas": [
    {
      "hypothesis": "Plain English description of the factor idea",
      "source": "URL or paper title",
      "expression": "Suggested WQ expression (if determined)",
      "family": "momentum / value / quality / volatility / size / sentiment / other",
      "confidence": "high / medium / low",
      "rationale": "Why this idea is promising for WQ implementation"
    }
  ]
}
```

**DISCOVERY FAILED**:
```json
{
  "type": "discovery_failed",
  "reasoning": "Explain what was tried and why nothing usable was found"
}
```"""


# ── IdeaScout ────────────────────────────────────────────────────────────────

class IdeaScout:
    """Autonomous web-based factor idea discovery.

    Uses web search + LLM to find, validate, and rank investment theses
    that can be implemented as WQ alpha factors.
    """

    def __init__(self, tools: WQTools, llm: LLMClient, log_dir: str | None = None):
        self.tools = tools
        self.llm = llm
        self._log_dir = Path(log_dir) if log_dir else None
        self._scout_exchange_counter = 0

    def discover(self, dataset_id: str = "", quiet: bool = False) -> list[dict]:
        """Run the discovery process.

        Returns a list of idea dicts, each with:
          {hypothesis, source, expression, family, confidence, rationale}
        Returns an empty list if discovery fails.
        """
        sys_prompt = IDEA_DISCOVERY_SYSTEM_PROMPT

        user_parts = []
        if dataset_id:
            user_parts.append(
                f"Dataset specified: {dataset_id}. "
                "Focus your search on ideas relevant to this dataset."
            )
        user_parts.append(
            "Search the web for factor ideas. Read promising sources. "
            "Validate candidate expressions against real WQ operators and fields. "
            "When you have 1-3 solid ideas, submit via type 'discovery_complete'."
        )

        messages: list[dict] = [
            {"role": "system", "content": sys_prompt},
            {"role": "user", "content": "\n".join(user_parts)},
        ]

        stall_count = 0
        for turn in range(MAX_DISCOVERY_TURNS):
            try:
                content = self.llm.chat(messages, temperature=0.5)
            except RuntimeError as exc:
                if not quiet:
                    print(f"  [LLM ERR] {exc}")
                stall_count += 1
                if stall_count >= MAX_DISCOVERY_STALL:
                    break
                continue

            messages.append({"role": "assistant", "content": content})

            # Log exchange if log_dir is set.
            if self._log_dir:
                self._scout_exchange_counter += 1
                log_exchange(
                    self._log_dir,
                    exchange_id=f"scout_{self._scout_exchange_counter}",
                    call_type="scout",
                    messages=messages[:-1],
                    response=content,
                    temperature=0.5,
                    agent="scout",
                )

            try:
                parsed = self._extract_json(content)
            except (ValueError, json.JSONDecodeError) as exc:
                messages.append({
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
                messages.append({
                    "role": "user",
                    "content": json.dumps(result, ensure_ascii=False, default=str),
                })
                stall_count = 0

            elif rtype == "discovery_complete":
                ideas = parsed.get("ideas", [])
                if ideas:
                    return ideas
                messages.append({
                    "role": "user",
                    "content": "Empty ideas list. Continue searching or use discovery_failed.",
                })
                stall_count += 1

            elif rtype == "discovery_failed":
                if not quiet:
                    print(f"  [DISCOVERY FAILED] {parsed.get('reasoning', '')[:200]}")
                return []

            else:
                messages.append({
                    "role": "user",
                    "content": f"Unknown type '{rtype}'. Use 'tool_call', 'discovery_complete', or 'discovery_failed'.",
                })
                stall_count += 1
                if stall_count >= MAX_DISCOVERY_STALL:
                    break

        return []

    # ── Internal helpers ─────────────────────────────────────────────────

    def _extract_json(self, text: str) -> dict:
        """Extract a JSON object from *text*, which may contain markdown fences."""
        # Try to find a JSON block inside ``` ... ``` fences first.
        match = re.search(r"```(?:json)?\s*\n?(.*?)```", text, re.DOTALL)
        if match:
            raw = match.group(1).strip()
        else:
            raw = text.strip()
        return json.loads(raw)

    def _execute_tool(self, parsed: dict) -> Any:
        """Execute a tool call and return the result."""
        tool = parsed.get("tool", "")
        args = parsed.get("args", {})

        tool_map = {
            "web_search": lambda: self.tools.web_search(
                args.get("query", ""), args.get("max_results", 10)
            ),
            "fetch_webpage": lambda: self.tools.fetch_webpage(
                args.get("url", ""), args.get("max_chars", 5000)
            ),
            "list_datasets": lambda: self.tools.list_datasets(),
            "list_fields": lambda: self.tools.list_fields(args.get("dataset_id", "")),
            "get_field_detail": lambda: self.tools.get_field_detail(
                args.get("field_id", ""), args.get("dataset_id", "")
            ),
            "get_dataset_detail": lambda: self.tools.get_dataset_detail(
                args.get("dataset_id", "")
            ),
            "list_all_operators": lambda: self.tools.list_all_operators(),
            "search_operators": lambda: self.tools.search_operators(
                args.get("keyword", "")
            ),
            "get_operator_detail": lambda: self.tools.get_operator_detail(
                args.get("name", "")
            ),
            "validate_expression": lambda: self.tools.validate_expression(
                args.get("expression", ""), args.get("dataset_id", "")
            ),
        }

        handler = tool_map.get(tool)
        if handler:
            try:
                return handler()
            except Exception as exc:
                return {"error": str(exc)}
        return {"error": f"Unknown tool: {tool}"}
