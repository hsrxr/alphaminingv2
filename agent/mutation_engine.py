"""
mutation_engine.py — Generate factor mutations from diagnostic analysis.

Takes an existing factor expression and its LLM diagnosis, and produces
modified versions (mutations) designed to address identified weaknesses.

Two mutation modes:
  1. Rule-based: deterministic transformations based on diagnosis type
  2. LLM-based: creative mutations using LLM for novel suggestions

Usage:
  python mutation_engine.py \\
      --expression "group_rank(ts_mean(subtract(A, B), 21), industry)" \\
      --sharpe 0.6 --turnover 0.8 --output-dir mutations
"""

import argparse
import json
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from agent.llm_client import LLMClient, load_api_key
from agent.expression_validator import (
    ExpressionValidator,
    _load_known_operators,
    WQ_OPERATORS_FILE,
)


# ─── Rule-based mutation strategies ─────────────────────────────────────────

def _wrap(expr: str, outer: str, *args: str) -> str:
    """Wrap *expr* inside an outer operator call.

    ``_wrap("rank(A)", "ts_mean", "5")`` → ``"ts_mean(rank(A), 5)"``
    """
    if args:
        return f"{outer}({expr}, {', '.join(args)})"
    return f"{outer}({expr})"


_MUTATION_RULES: list[dict] = [
    # ── Smoothing / noise reduction ──────────────────────────────────────
    {
        "id": "add_ts_mean_5",
        "description": "Add ts_mean smoothing (window=5) for noise reduction",
        "expected_impact": "Reduces turnover, potentially lowers Sharpe slightly",
        "tags": ["high_turnover", "noisy"],
        "apply": lambda e: _wrap(e, "ts_mean", "5"),
    },
    {
        "id": "add_ts_mean_21",
        "description": "Add ts_mean smoothing (window=21) for stronger noise reduction",
        "expected_impact": "Significantly reduces turnover, may improve Sharpe if signal is persistent",
        "tags": ["high_turnover", "noisy"],
        "apply": lambda e: _wrap(e, "ts_mean", "21"),
    },
    {
        "id": "add_ts_decay_linear_5",
        "description": "Replace/add ts_decay_linear smoothing (window=5) for recent-weighted smoothing",
        "expected_impact": "Less lag than ts_mean, better for fast signals",
        "tags": ["high_turnover", "noisy"],
        "apply": lambda e: _wrap(e, "ts_decay_linear", "5"),
    },
    # ── Normalization / cross-section ────────────────────────────────────
    {
        "id": "add_ts_zscore_21",
        "description": "Add ts_zscore normalization (window=21) to standardize signal",
        "expected_impact": "Improves fitness, reduces outlier impact",
        "tags": ["low_sharpe", "poor_fitness", "outliers"],
        "apply": lambda e: _wrap(e, "ts_zscore", "21"),
    },
    {
        "id": "add_rank",
        "description": "Add cross-sectional rank transform",
        "expected_impact": "Makes signal distribution uniform, improves ranking perspective",
        "tags": ["low_sharpe", "skewed"],
        "apply": lambda e: _wrap(e, "rank"),
    },
    {
        "id": "add_group_rank_sector",
        "description": "Add sector-relative ranking",
        "expected_impact": "Neutralizes sector effects, often improves Sharpe",
        "tags": ["low_sharpe", "sector_bias"],
        "apply": lambda e: _wrap(e, "group_rank", "sector"),
    },
    {
        "id": "add_group_neutralize_industry",
        "description": "Add industry-neutralization",
        "expected_impact": "Removes industry-specific noise",
        "tags": ["low_sharpe", "industry_bias"],
        "apply": lambda e: _wrap(e, "group_neutralize", "industry"),
    },
    # ── Operator substitution ────────────────────────────────────────────
    {
        "id": "replace_ts_mean_with_ts_rank",
        "description": "Replace ts_mean with ts_rank for non-linear time-series perspective",
        "expected_impact": "Captures rank-based trends, less sensitive to magnitude",
        "tags": ["low_sharpe", "nonlinear"],
        "apply": lambda e: e.replace("ts_mean(", "ts_rank(", 1),
    },
    {
        "id": "replace_ts_mean_with_ts_zscore",
        "description": "Replace ts_mean with ts_zscore for adaptive normalization",
        "expected_impact": "Adaptive to changing volatility",
        "tags": ["low_sharpe", "regime_change"],
        "apply": lambda e: e.replace("ts_mean(", "ts_zscore(", 1),
    },
    # ── Combination ──────────────────────────────────────────────────────
    {
        "id": "add_inverse",
        "description": "Add inverse transform to capture contrarian signal",
        "expected_impact": "May flip sign, useful if factor works in reverse",
        "tags": ["negative_sharpe", "contrarian"],
        "apply": lambda e: _wrap(e, "inverse"),
    },
    {
        "id": "add_log",
        "description": "Add log transform to normalize skewed distribution",
        "expected_impact": "Reduces outlier influence",
        "tags": ["skewed", "outliers"],
        "apply": lambda e: _wrap(e, "log"),
    },
]


# ─── Mutation engine ────────────────────────────────────────────────────────

class MutationEngine:
    """Generate factor mutations based on diagnosis analysis.

    Usage::

        engine = MutationEngine()
        mutations = engine.rule_based_mutate(expression, diagnosis)
        # or
        mutations = await engine.llm_based_mutate(expression, diagnosis, llm_client)
    """

    def __init__(self, validator: Optional[ExpressionValidator] = None):
        self.validator = validator or ExpressionValidator()
        self.rules = _MUTATION_RULES

    # ── Rule-based ───────────────────────────────────────────────────────

    def rule_based_mutate(
        self,
        expression: str,
        diagnosis: dict | None = None,
        sharpe: float | None = None,
        turnover: float | None = None,
        fitness: float | None = None,
        max_mutations: int = 6,
    ) -> list[dict]:
        """Apply deterministic rule-based mutations to an expression.

        Mutations are selected based on the diagnosis tags and/or metric
        thresholds.  Results are validated before being returned.
        """
        tags = self._diagnosis_tags(diagnosis)

        # Metric-based heuristics.
        if turnover is not None and turnover > 0.6:
            tags.append("high_turnover")
        if sharpe is not None and sharpe < 0.5:
            tags.append("low_sharpe")
        if fitness is not None and fitness < 0.3:
            tags.append("poor_fitness")
        if sharpe is not None and sharpe < 0:
            tags.append("negative_sharpe")

        # Select matching rules (deduplicated).
        seen_ids: set[str] = set()
        matched: list[dict] = []
        for rule in self.rules:
            if any(tag in rule.get("tags", []) for tag in tags):
                if rule["id"] not in seen_ids:
                    seen_ids.add(rule["id"])
                    matched.append(rule)

        # Generate and validate expressions.
        mutations: list[dict] = []
        for rule in matched[:max_mutations]:
            try:
                new_expr = rule["apply"](expression)
            except Exception as exc:
                continue

            # Validate the mutated expression.
            vr = self.validator.validate(new_expr)
            mutations.append({
                "mutation_id": rule["id"],
                "description": rule["description"],
                "expected_impact": rule.get("expected_impact", ""),
                "original_expression": expression,
                "mutated_expression": new_expr,
                "valid": vr.valid,
                "validation_errors": vr.errors,
                "validation_warnings": vr.warnings,
            })

        return mutations

    @staticmethod
    def _diagnosis_tags(diagnosis: dict | None) -> list[str]:
        """Extract semantic tags from an LLM diagnosis dict."""
        if not diagnosis:
            return []

        tags: list[str] = []
        weaknesses = [w.lower() for w in diagnosis.get("weaknesses", [])]
        strengths = [s.lower() for s in diagnosis.get("strengths", [])]

        # Keyword → tag mapping.
        keyword_tags = [
            ("turnover", "high_turnover"),
            ("noisy", "noisy"),
            ("volatile", "volatile"),
            ("noise", "noisy"),
            ("outlier", "outliers"),
            ("skew", "skewed"),
            ("nonlinear", "nonlinear"),
            ("non-linear", "nonlinear"),
            ("regime", "regime_change"),
            ("sector bias", "sector_bias"),
            ("industry bias", "industry_bias"),
            ("contrarian", "contrarian"),
        ]

        for weakness in weaknesses:
            for keyword, tag in keyword_tags:
                if keyword in weakness and tag not in tags:
                    tags.append(tag)

        return tags

    # ── LLM-based ────────────────────────────────────────────────────────

    def llm_based_mutate(
        self,
        expression: str,
        diagnosis: dict | None = None,
        sharpe: float | None = None,
        turnover: float | None = None,
        dataset_id: str | None = None,
        llm: Optional[LLMClient] = None,
        n_suggestions: int = 3,
    ) -> list[dict]:
        """Use LLM to suggest creative mutations based on full diagnosis."""
        if llm is None:
            llm = LLMClient()

        prompt = self._build_llm_prompt(
            expression, diagnosis, sharpe, turnover, dataset_id
        )

        try:
            raw = llm.chat_structured(prompt)
        except Exception as exc:
            return [{
                "mutation_id": "llm_error",
                "description": f"LLM call failed: {exc}",
                "mutated_expression": expression,
                "valid": False,
                "validation_errors": [str(exc)],
            }]

        suggestions = raw.get("mutations", []) if isinstance(raw, dict) else []
        return self._validate_mutations(suggestions, expression, n_suggestions)

    def _build_llm_prompt(
        self,
        expression: str,
        diagnosis: dict | None,
        sharpe: float | None,
        turnover: float | None,
        dataset_id: str | None,
    ) -> list[dict]:
        """Build the LLM prompt for creative mutation suggestions."""
        diagnosis_block = ""
        if diagnosis:
            diagnosis_block = (
                f"## Diagnosis\n"
                f"Verduct: {diagnosis.get('verdict', '?')}\n"
                f"Weaknesses: {', '.join(diagnosis.get('weaknesses', []))}\n"
                f"Suggestions: {', '.join(s.get('description', '') for s in diagnosis.get('improvement_suggestions', []))}\n"
            )

        metrics_block = (
            f"## Current Metrics\n"
            f"Sharpe: {sharpe or 'N/A'}\n"
            f"Turnover: {turnover or 'N/A'}\n"
        )

        system = """\
You are an expert quant engineer specializing in WQ FASTEXPR alpha factor development.
Given an existing factor expression and its diagnostic analysis, suggest concrete
mutations that could improve performance.

Each mutation must:
- Be a syntactically valid WQ FASTEXPR expression
- Use only operators from the 50 standard WQ operators
- Have a clear economic or statistical motivation
- Address a specific weakness from the diagnosis

Respond in this JSON format:
{
  "mutations": [
    {
      "id": "short_id",
      "description": "What this mutation does and why",
      "mutated_expression": "the modified WQ expression",
      "expected_impact": "How metrics should change"
    }
  ]
}"""

        user = f"""\
## Original Expression
{expression}

{diagnosis_block}
{metrics_block}

Suggest {3} distinct mutations. Respond with the JSON only.\
"""

        return [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]

    def _validate_mutations(
        self,
        suggestions: list[dict],
        original_expression: str,
        max_count: int,
    ) -> list[dict]:
        """Validate LLM-suggested mutations."""
        validated: list[dict] = []
        for i, sug in enumerate(suggestions[:max_count]):
            new_expr = sug.get("mutated_expression", original_expression)
            vr = self.validator.validate(new_expr)
            validated.append({
                "mutation_id": sug.get("id", f"llm_{i}"),
                "description": sug.get("description", ""),
                "expected_impact": sug.get("expected_impact", ""),
                "original_expression": original_expression,
                "mutated_expression": new_expr,
                "valid": vr.valid,
                "validation_errors": vr.errors,
                "validation_warnings": vr.warnings,
            })
        return validated

    # ── Output ───────────────────────────────────────────────────────────

    @staticmethod
    def mutations_to_template(
        mutations: list[dict],
        dataset_id: str = "",
        output_dir: str = "mutations",
        template_id: str = "TPL_MUTATION_V1",
    ) -> Path | None:
        """Write validated mutations as a template catalog file that main.py can consume.

        Only valid mutations are included.  Each mutation becomes a separate
        template with a fixed expression (no slots — a concrete factor ready
        for backtesting).
        """
        valid = [m for m in mutations if m.get("valid")]
        if not valid:
            return None

        # Build a one-off template catalog.
        templates = []
        for m in valid:
            tid = f"TPL_MUT_{m['mutation_id'].upper()}_V1"
            templates.append({
                "template_id": tid,
                "source_name": m["mutation_id"],
                "description": m["description"][:120],
                "expression": m["mutated_expression"],
                "core_slots": [],
                "applicable_datasets": [dataset_id] if dataset_id else [],
                "slots": {},
            })

        catalog = {
            "naming": {"regex": "^TPL_[A-Z0-9_]+_V[0-9]+$"},
            "templates": templates,
        }

        out_path = Path(output_dir) / dataset_id
        out_path.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = out_path / f"mutations_{timestamp}.json"
        file_path.write_text(
            json.dumps(catalog, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"\nMutations saved as template catalog: {file_path}")
        return file_path


# ─── CLI ────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate factor mutations from diagnostic analysis.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--expression", required=True, help="Original WQ expression.")
    parser.add_argument("--sharpe", type=float, default=None, help="Mean Sharpe ratio.")
    parser.add_argument("--turnover", type=float, default=None, help="Mean turnover.")
    parser.add_argument("--fitness", type=float, default=None, help="Mean fitness.")
    parser.add_argument("--diagnosis-file", default="", help="Path to LLM diagnosis JSON.")
    parser.add_argument("--dataset-id", default="", help="Dataset context for field validation.")
    parser.add_argument("--output-dir", default="mutations", help="Output directory.")
    parser.add_argument("--mode", choices=["rule", "llm", "both"], default="rule",
                        help="Mutation mode.")
    parser.add_argument("--api-key", default="", help="DeepSeek API key (for llm mode).")
    parser.add_argument("--model", default="deepseek-chat", help="LLM model.")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    engine = MutationEngine()

    # Load diagnosis if provided.
    diagnosis = None
    if args.diagnosis_file:
        try:
            diagnosis = json.loads(Path(args.diagnosis_file).read_text(encoding="utf-8"))
            # Drill down to analysis key if this is a full result file.
            if "analysis" in diagnosis:
                diagnosis = diagnosis["analysis"]
        except (OSError, json.JSONDecodeError) as exc:
            print(f"[WARN] Cannot load diagnosis file: {exc}")

    all_mutations: list[dict] = []

    # Rule-based.
    if args.mode in ("rule", "both"):
        print("Generating rule-based mutations...")
        rule_muts = engine.rule_based_mutate(
            expression=args.expression,
            diagnosis=diagnosis,
            sharpe=args.sharpe,
            turnover=args.turnover,
            fitness=args.fitness,
        )
        all_mutations.extend(rule_muts)
        print(f"  {len(rule_muts)} rule-based mutation(s) generated.")
        for m in rule_muts:
            status = _c("\033[92m", "OK") if m["valid"] else _c("\033[91m", "INVALID")
            print(f"    {status} {m['mutation_id']}: {m['description'][:80]}")

    # LLM-based.
    if args.mode in ("llm", "both"):
        print("\nGenerating LLM-based mutations...")
        api_key = args.api_key or load_api_key()
        llm = LLMClient(api_key=api_key, model=args.model)
        llm_muts = engine.llm_based_mutate(
            expression=args.expression,
            diagnosis=diagnosis,
            sharpe=args.sharpe,
            turnover=args.turnover,
            dataset_id=args.dataset_id,
            llm=llm,
        )
        all_mutations.extend(llm_muts)
        print(f"  {len(llm_muts)} LLM-based mutation(s) generated.")
        for m in llm_muts:
            status = _c("\033[92m", "OK") if m["valid"] else _c("\033[91m", "INVALID")
            print(f"    {status} {m['mutation_id']}: {m['description'][:80]}")

    # Output.
    if not all_mutations:
        print("\nNo mutations generated.")
        return

    dataset_id = args.dataset_id if args.dataset_id else "unknown"
    result = engine.mutations_to_template(
        all_mutations,
        dataset_id=dataset_id,
        output_dir=args.output_dir,
    )

    if result:
        valid_count = sum(1 for m in all_mutations if m.get("valid"))
        print(f"\nSummary: {valid_count}/{len(all_mutations)} mutations valid.")
        print(f"Output:   {result}")
    else:
        print("\nNo valid mutations to save.")


def _c(code: str, text: str) -> str:
    import sys
    return f"{code}{text}\033[0m" if sys.stdout.isatty() else text


if __name__ == "__main__":
    main()
