"""param_optimizer.py — Systematic parameter search for WQ factor expressions.

Two strategies are provided:

* **Script mode** (``ParamOptimizer``): deterministic hierarchical greedy search
  over the settings space.  LLM-free, fully reproducible.
* **LLM mode** (``LLMOptimizer``): uses the language model to suggest the next
  settings combination to try, based on past results.
"""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from agent.llm_logger import log_exchange
from agent.multi.results_store import ResultsStore
from agent.wq_tools import WQTools, SETTINGS_SCHEMA

logger = logging.getLogger(__name__)


# ── Shared types ──────────────────────────────────────────────────────────────

@dataclass
class OptimizerSuggestion:
    """A single settings combination for the Orchestrator to test."""
    expression: str
    settings: dict[str, Any]
    rationale: str = ""
    layer: str = ""


# ── Search-space definition ───────────────────────────────────────────────────

_LAYERS: list[dict] = [
    {
        "name": "neutralization",
        "params": {
            "neutralization": ["MARKET", "INDUSTRY", "SECTOR", "SUBINDUSTRY", "NONE"],
        },
        "description": "Risk neutralization level — try all options, pick best.",
    },
    {
        "name": "decay",
        "params": {
            "decay": [1, 2, 3, 5, 10, 22, 44],
        },
        "description": "Signal decay half-life — vary from fast to very smooth.",
    },
    {
        "name": "delay_truncation",
        "params": {
            "delay": [0, 1],
            "truncation": [0.0, 0.05, 0.08, 0.10, 0.15],
        },
        "description": "Delay and concentration — two parameters together (small space).",
    },
]

# Layer 0: nanHandling quick check (useful for sparse datasets).
_LAYERS_EXTRA: list[dict] = [
    {
        "name": "nanHandling",
        "params": {
            "nanHandling": ["ON", "OFF"],
        },
        "description": "NaN handling — try both and see which works better.",
    },
]


# ── Abstract base ─────────────────────────────────────────────────────────────

class BaseOptimizer(ABC):
    """Abstract optimizer for a single factor expression."""

    def __init__(self, tools: WQTools, results_store: ResultsStore):
        self.tools = tools
        self.results_store = results_store

    @abstractmethod
    def next_suggestions(self, expression: str,
                         known_results: list[dict] | None = None,
                         max_suggestions: int = 3) -> list[OptimizerSuggestion]:
        """Return the next batch of settings to test for *expression*.

        Args:
            expression: The factor expression to optimise.
            known_results: All past results for this expression (may include
                results from the Explorer phase or earlier rounds).
            max_suggestions: Maximum number of suggestions to return (respects
                the available WQ slot count).

        Returns:
            A list of ``OptimizerSuggestion`` items, best-first.
        """
        ...


# ── Script-mode: hierarchical greedy ─────────────────────────────────────────

class ParamOptimizer(BaseOptimizer):
    """Script-driven hierarchical greedy parameter search.

    Strategy
    --------
    Layer 1 — neutralization (5 values): find the best group to neutralise by.
    Layer 2 — decay (7 values):          find the best signal decay speed.
    Layer 3 — delay + truncation (10):   fine-tune trading delay and weight cap.

    At each layer the best-performing setting from the previous layer is frozen,
    and only the current layer's parameters are swept.  This limits the search
    to ~5 + 7 + 10 = 22 combinations instead of 5×7×10 = 350.
    """

    def __init__(self, tools: WQTools, results_store: ResultsStore,
                 layers: list[dict] | None = None,
                 early_stop_threshold: int = 2):
        super().__init__(tools, results_store)
        self.layers = layers or _LAYERS
        self.early_stop_threshold = early_stop_threshold
        self._layer_index: int = 0
        self._frozen_settings: dict[str, Any] = {}

    def reset(self) -> None:
        """Reset the search state (call when starting a new expression)."""
        self._layer_index = 0
        self._frozen_settings = {}

    def next_suggestions(self, expression: str,
                         known_results: list[dict] | None = None,
                         max_suggestions: int = 3) -> list[OptimizerSuggestion]:
        known = known_results or self.results_store.get_by_expression(expression)
        suggestions: list[OptimizerSuggestion] = []

        # Skip layers already completed.
        while self._layer_index < len(self.layers):
            layer = self.layers[self._layer_index]
            candidates = self._build_candidates(layer, known)
            # Filter out already-tested combinations.
            untested = [c for c in candidates
                        if not self.results_store.has_been_tested(expression, c)]
            if not untested:
                # All combinations in this layer tested → pick best, advance.
                self._finalise_layer(known)
                continue

            # Return up to max_suggestions.
            for combo in untested[:max_suggestions]:
                self._frozen_settings.update(combo)
                suggestions.append(OptimizerSuggestion(
                    expression=expression,
                    settings=dict(self._frozen_settings),
                    rationale=f"Layer: {layer['name']} — {layer['description']}",
                    layer=layer["name"],
                ))

            # If we filled the suggestion quota, stop (Orchestrator will call
            # us again when slots free up).
            if len(suggestions) >= max_suggestions:
                break

        return suggestions

    # ── internal ─────────────────────────────────────────────────────────

    def _build_candidates(self, layer: dict,
                          known_results: list[dict]) -> list[dict]:
        """Build the Cartesian product of parameter values for *layer*,
        starting from the current frozen settings."""
        import itertools

        param_names = list(layer["params"].keys())
        param_values = [layer["params"][p] for p in param_names]

        candidates: list[dict] = []
        for values in itertools.product(*param_values):
            combo = dict(self._frozen_settings)
            for name, value in zip(param_names, values):
                combo[name] = value
            candidates.append(combo)
        return candidates

    def _finalise_layer(self, known_results: list[dict]) -> None:
        """Pick the best result from the just-completed layer and freeze it."""
        layer = self.layers[self._layer_index]

        best_sharpe = -999
        best_combo: dict | None = None

        for r in known_results:
            if r.get("sharpe") is None:
                continue
            settings = r.get("settings", {})
            # Only consider results within this layer's parameters.
            if any(settings.get(k) != v
                   for k, v in self._frozen_settings.items()
                   if k not in layer["params"]):
                continue
            s = r["sharpe"]
            if s > best_sharpe:
                best_sharpe = s
                best_combo = settings

        if best_combo:
            # Update frozen settings with this layer's best values.
            for k in layer["params"]:
                if k in best_combo:
                    self._frozen_settings[k] = best_combo[k]

        self._layer_index += 1
        logger.info(
            "Layer '%s' done — best sharpe=%.2f, frozen=%s",
            layer["name"], best_sharpe, self._frozen_settings,
        )

    @property
    def is_converged(self) -> bool:
        """True when all layers have been swept."""
        return self._layer_index >= len(self.layers)

    @property
    def current_state(self) -> dict:
        return {
            "layer_index": self._layer_index,
            "frozen_settings": dict(self._frozen_settings),
            "is_converged": self.is_converged,
        }


# ── LLM-mode: reuse the existing DirectAgent analysis logic ───────────────────

class LLMOptimizer(BaseOptimizer):
    """LLM-driven parameter optimizer.

    This is a thin wrapper that reuses the prompt templates from
    ``DirectAgent._analyse_and_improve()`` to let the LLM decide which
    settings combination to try next.
    """

    def __init__(self, tools: WQTools, results_store: ResultsStore,
                 llm_client, operators_list: list[dict] | None = None,
                 log_dir: str | None = None):
        super().__init__(tools, results_store)
        self.llm = llm_client
        self.operators_list = operators_list or []
        self._log_dir = Path(log_dir) if log_dir else None
        self._optimizer_exchange_counter = 0

    def next_suggestions(self, expression: str,
                         known_results: list[dict] | None = None,
                         max_suggestions: int = 3) -> list[OptimizerSuggestion]:
        """Let the LLM propose the next settings to try."""
        known = known_results or self.results_store.get_by_expression(expression)
        if not known:
            # No data yet — suggest the default settings.
            defaults = {
                k: v["default"] for k, v in SETTINGS_SCHEMA.items()
                if v.get("_api", True) and k != "language"
            }
            return [OptimizerSuggestion(
                expression=expression,
                settings=defaults,
                rationale="Initial default settings (no prior results).",
                layer="initial",
            )]

        prompt = self._build_prompt(expression, known)
        response = self.llm.chat(prompt, temperature=0.3)

        # Log exchange if log_dir is set.
        if self._log_dir:
            self._optimizer_exchange_counter += 1
            exchange_id = f"optimizer_{self._optimizer_exchange_counter}"
            prompt_text = prompt if isinstance(prompt, str) else str(prompt)
            response_text = response if isinstance(response, str) else str(response)
            log_exchange(
                self._log_dir,
                exchange_id=exchange_id,
                call_type="optimize",
                messages=[{"role": "user", "content": prompt_text}],
                response=response_text,
                temperature=0.3,
                agent="optimizer",
            )

        suggestions = self._parse_suggestions(response, expression, max_suggestions)
        return suggestions or self._fallback_default(expression)

    def _build_prompt(self, expression: str, known_results: list[dict]) -> str:
        lines = [
            "You are optimising the settings for a single WQ factor expression.",
            "",
            f"Expression: {expression}",
            "",
            "Past results for this expression:",
        ]
        for r in known_results[-10:]:  # last 10 results
            s = r.get("settings", {})
            sharpe = r.get("sharpe", "?")
            turnover = r.get("turnover", "?")
            fitness = r.get("fitness", "?")
            sett_str = ", ".join(f"{k}={v}" for k, v in sorted(s.items())
                                 if v is not None and k != "language")
            lines.append(f"  settings: {sett_str}  sharpe={sharpe}  turnover={turnover}  fitness={fitness}")

        lines.extend([
            "",
            "Respond with a JSON object:",
            '  {"suggestions": [',
            '    {"settings": {"param": "value", ...}, "rationale": "..."}',
            "  ]}",
            "",
            "Available parameters and their allowed values:",
            "  neutralization: MARKET, INDUSTRY, SECTOR, SUBINDUSTRY, NONE",
            "  decay: any positive integer (commonly 1, 2, 3, 5, 10, 22, 44)",
            "  delay: 0 or 1",
            "  truncation: 0.0 to 1.0 (step 0.01)",
            "  nanHandling: ON or OFF",
            "  universe: TOP3000, TOP2000, TOP1000, TOP500, TOP200",
            "",
            "Rules:",
            "  1. Suggest settings you haven't tried yet (no repeats).",
            "  2. Vary one parameter at a time to isolate its effect.",
            "  3. Prefer lower turnover (aim for < 0.30).",
            "  4. If turnover is high, try longer decay.",
            "  5. If fitness is low, try stronger neutralization.",
        ])
        return "\n".join(lines)

    def _parse_suggestions(self, response: dict,
                           expression: str,
                           max_suggestions: int) -> list[OptimizerSuggestion]:
        try:
            content = response.get("content", "{}")
            # Try to extract JSON from the response.
            import re
            json_match = re.search(r'\{.*"suggestions".*\}', content, re.DOTALL)
            if json_match:
                data = json.loads(json_match.group())
            else:
                data = json.loads(content)
        except (json.JSONDecodeError, AttributeError):
            return []

        raw = data.get("suggestions", [])
        suggestions: list[OptimizerSuggestion] = []
        for item in raw[:max_suggestions]:
            settings = item.get("settings", {})
            if not settings:
                continue
            suggestions.append(OptimizerSuggestion(
                expression=expression,
                settings=settings,
                rationale=item.get("rationale", ""),
                layer="llm",
            ))
        return suggestions

    def _fallback_default(self, expression: str) -> list[OptimizerSuggestion]:
        defaults = {
            k: v["default"] for k, v in SETTINGS_SCHEMA.items()
            if v.get("_api", True) and k != "language"
        }
        return [OptimizerSuggestion(
            expression=expression,
            settings=defaults,
            rationale="Fallback: default settings.",
            layer="fallback",
        )]
