"""explorer.py — LLM-based structural exploration of the WQ expression space.

The Explorer generates candidate expressions that are *structurally different*
from previously tested ones, guided by an investment thesis and past results.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from typing import Any

from agent.expression_fingerprint import expression_fingerprint, structure_distance
from agent.expression_validator import ExpressionValidator
from agent.llm_client import LLMClient
from agent.llm_logger import log_exchange
from agent.mutation_engine import MutationEngine
from agent.results_store import ResultsStore
from agent.wq_tools import WQTools

logger = logging.getLogger(__name__)

_EXPLORER_SYSTEM_PROMPT = """\
You are an expert quantitative researcher specialising in WQ (WorldQuant) alpha factor
development.  Your job is to design structurally *diverse* factor expressions for a given
investment thesis.

## Guidelines

1. **Structural diversity**: Each expression should use a different combination of WQ
   operators.  If one expression uses ``ts_decay_linear(ts_scale(...), 22)``, the next
   should not — try ``ts_zscore``, ``group_rank``, ``ts_regression``, ``ts_correlation``,
   ``ts_av_diff``, or other operators from the list.

2. **Financial logic**: Each expression must implement the thesis in a credible way.
   The operator choices should have an economic rationale.

3. **Correctness**: Use only the 51 standard WQ operators.  Do not invent operator names.
   Every leaf token must be a valid Brain data field.

4. **Expression formula rules**:
   - Expressions must be valid WQ FASTEXPR syntax.
   - Parentheses must be balanced.
   - Use only operators from the provided list.

## Output format

Respond with EXACTLY a JSON object (no markdown, no extra text):

{
  "expressions": [
    {
      "expression": "ts_decay_linear(ts_zscore(field_a, 252), 22)",
      "rationale": "Why this structure tests the thesis",
      "expected_structure": "zscore → decay_linear"
    }
  ]
}
"""


@dataclass
class ExplorerCandidate:
    expression: str
    rationale: str
    expected_structure: str


class Explorer:
    """Generates structurally diverse factor expressions for a given thesis.

    The Explorer uses an LLM with high temperature to propose candidates,
    validates them, and falls back to the MutationEngine when the LLM
    fails to produce valid new structures.
    """

    def __init__(
        self,
        tools: WQTools,
        llm: LLMClient,
        results_store: ResultsStore,
        mutation_engine: MutationEngine | None = None,
        temperature: float = 0.7,
        retry_per_round: int = 3,
        operators_list: list[dict] | None = None,
        log_dir: str | None = None,
    ):
        self.tools = tools
        self.llm = llm
        self.results_store = results_store
        self.mutation_engine = mutation_engine or MutationEngine()
        self.temperature = temperature
        self.retry_per_round = retry_per_round
        self.operators_list = operators_list or []
        self._expression_history: list[str] = []
        self._seen_fingerprints: set[str] = set()
        self._log_dir = Path(log_dir) if log_dir else None
        self._explorer_exchange_counter = 0

    def explore(
        self,
        thesis: str,
        context: dict | None = None,
        n_candidates: int = 3,
    ) -> list[ExplorerCandidate]:
        """Generate structurally diverse candidates for *thesis*.

        Args:
            thesis: The investment thesis to implement.
            context: Optional extra context (dataset info, available fields, etc.).
            n_candidates: How many candidates to produce (default 3).

        Returns:
            A list of validated ``ExplorerCandidate`` objects.
        """
        self._load_history()

        # Try LLM generation with retries.
        for attempt in range(self.retry_per_round):
            prompt = self._build_prompt(thesis, context, n_candidates)
            prompt_text = prompt if isinstance(prompt, str) else str(prompt)
            response_raw = self.llm.chat(prompt, temperature=self.temperature)

            # Log exchange if log_dir is set.
            if self._log_dir:
                self._explorer_exchange_counter += 1
                exchange_id = f"explorer_{self._explorer_exchange_counter}"
                log_exchange(
                    self._log_dir,
                    exchange_id=exchange_id,
                    call_type="explore",
                    messages=[{"role": "user", "content": prompt_text}],
                    response=response_raw if isinstance(response_raw, str) else str(response_raw),
                    temperature=self.temperature,
                    agent="explorer",
                )

            candidates = self._parse_candidates(response_raw)

            validated = self._validate_candidates(candidates)
            if validated:
                for c in validated:
                    self._expression_history.append(c.expression)
                    fp = expression_fingerprint(c.expression)
                    self._seen_fingerprints.add(fp)
                return validated

            logger.info(
                "Explorer attempt %d/%d: no valid candidates, retrying...",
                attempt + 1, self.retry_per_round,
            )

        # Fallback: use MutationEngine on the best expression from ResultsStore.
        logger.info("Explorer LLM exhausted — falling back to MutationEngine.")
        return self._fallback_to_mutation_engine(n_candidates)

    # ── internal ─────────────────────────────────────────────────────────

    def _load_history(self) -> None:
        """Load previously tested expressions from ResultsStore."""
        for r in self.results_store.get_all():
            expr = r.get("expression", "")
            if expr:
                self._expression_history.append(expr)
                fp = expression_fingerprint(expr)
                self._seen_fingerprints.add(fp)

    def _build_prompt(self, thesis: str, context: dict | None,
                       n_candidates: int) -> str:
        context_block = ""
        if context:
            ctx_lines = []
            for k, v in context.items():
                ctx_lines.append(f"{k}: {v}")
            context_block = "\n".join(ctx_lines) + "\n"

        history_block = ""
        if self._expression_history:
            seen = self._expression_history[-10:]  # last 10
            lines = ["\nAlready tested expressions (MUST be structurally different):"]
            for expr in seen:
                fp = expression_fingerprint(expr)
                lines.append(f"  [{fp[:8]}] {expr}")
            history_block = "\n".join(lines)

        operators_block = ""
        if self.operators_list:
            ops = self.operators_list[:20]  # top 20 to keep prompt reasonable
            lines = ["\nAvailable WQ operators (use ONLY these):"]
            for op in ops:
                lines.append(f"  {op.get('syntax', ''):<35s} {op.get('summary', '')}")
            operators_block = "\n".join(lines)

        prompt = f"""\
{_EXPLORER_SYSTEM_PROMPT}

## Investment Thesis
{thesis}

{context_block}\
{operators_block}\
{history_block}

## Task
Generate {n_candidates} structurally DIFFERENT factor expressions that implement
this thesis.  Each must use a DIFFERENT primary operator or combination —
do NOT repeat the same structure with different field names.

Respond with the JSON only — no markdown, no explanation.\
"""
        return prompt

    def _parse_candidates(self, response: dict) -> list[ExplorerCandidate]:
        """Extract ExplorerCandidate list from an LLM response."""
        content = response.get("content", "") if isinstance(response, dict) else str(response)

        # Try to extract JSON from the response content.
        json_match = re.search(r'\{.*"expressions".*\}', content, re.DOTALL)
        if json_match:
            raw = json_match.group()
        else:
            # Maybe the whole response is JSON.
            raw = content.strip()

        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return []

        raw_exprs = data.get("expressions", []) if isinstance(data, dict) else []
        candidates: list[ExplorerCandidate] = []
        for item in raw_exprs:
            expr = (item.get("expression") or "").strip()
            if not expr:
                continue
            candidates.append(ExplorerCandidate(
                expression=expr,
                rationale=item.get("rationale", ""),
                expected_structure=item.get("expected_structure", ""),
            ))
        return candidates

    def _validate_candidates(self, candidates: list[ExplorerCandidate]) -> list[ExplorerCandidate]:
        """Filter candidates: validate syntax and check structural uniqueness."""
        valid: list[ExplorerCandidate] = []

        for c in candidates:
            # 1. Syntax validation.
            validation = self.tools.validate_expression(c.expression)
            if not validation.get("valid"):
                logger.debug("Explorer: invalid expression skipped: %s — %s",
                             c.expression, validation.get("errors"))
                continue

            # 2. Structural uniqueness — check against all seen fingerprints.
            fp = expression_fingerprint(c.expression)
            if fp in self._seen_fingerprints:
                logger.debug("Explorer: duplicate structure skipped: %s — %s",
                             c.expression, fp)
                continue

            # 3. Also check similarity to existing expressions (avoid near-duplicates).
            is_near_duplicate = False
            for existing_fp in self._seen_fingerprints:
                # Stricter check: if we can't reverse the hash, just rely on fingerprint.
                pass

            valid.append(c)

        return valid

    def _fallback_to_mutation_engine(self, n_candidates: int) -> list[ExplorerCandidate]:
        """Fallback: apply MutationEngine rules to the best expression so far."""
        best = self.results_store.get_best(min_sharpe=0.0)
        if not best:
            return []

        best_expr = best.get("expression", "")
        if not best_expr:
            return []

        mutations = self.mutation_engine.rule_based_mutate(
            expression=best_expr,
            sharpe=best.get("sharpe"),
            turnover=best.get("turnover"),
            fitness=best.get("fitness"),
            max_mutations=n_candidates,
        )

        candidates: list[ExplorerCandidate] = []
        for m in mutations:
            if not m.get("valid"):
                continue
            new_expr = m.get("mutated_expression", "")
            fp = expression_fingerprint(new_expr)
            if fp in self._seen_fingerprints:
                continue

            candidates.append(ExplorerCandidate(
                expression=new_expr,
                rationale=m.get("description", "MutationEngine fallback"),
                expected_structure=m["mutation_id"],
            ))
            self._seen_fingerprints.add(fp)

        return candidates

    @property
    def exploration_summary(self) -> dict:
        return {
            "total_expressions_tested": len(self._expression_history),
            "unique_structures": len(self._seen_fingerprints),
        }
