"""
expression_validator.py — Validate WQ FASTEXPR expressions.

Ensures LLM-generated expressions are safe before entering the backtest
pipeline.  Checks:
  - Parenthesis balance
  - Operator names exist in the known WQ operator database
  - Data field references exist in the dataset's cached field list
  - No obviously malformed constructs

Usage:
  python expression_validator.py \\
      --expression "group_rank(ts_mean(field, 21), industry)" \\
      --dataset-id pv13
"""

import argparse
import json
import re
from pathlib import Path


_PROJECT_ROOT = Path(__file__).resolve().parent.parent
WQ_OPERATORS_FILE = _PROJECT_ROOT / "wq_operators_cleaned.json"
DATAFIELDS_CACHE_DIR = _PROJECT_ROOT / "datafields_cache"


# ─── Operator helpers ───────────────────────────────────────────────────────

def _extract_operator_name(syntax: str) -> str:
    match = re.match(r"([A-Za-z_][A-Za-z0-9_]*)", syntax)
    if not match:
        raise ValueError(f"Cannot parse operator name from syntax: {syntax}")
    return match.group(1)


def _load_known_operators(path: Path = WQ_OPERATORS_FILE) -> dict[str, dict]:
    """Load known WQ operators keyed by name.

    Returns a dict like ``{"ts_mean": {"syntax": "ts_mean(x, d)", ...}, ...}``.
    """
    if not path.exists():
        return {}

    with open(path, encoding="utf-8") as fh:
        rows = json.load(fh)

    operators: dict[str, dict] = {}
    for row in rows:
        syntax = row.get("operator_syntax", "")
        name = _extract_operator_name(syntax)
        operators[name] = {
            "syntax": syntax,
            "summary": row.get("summary", ""),
        }
    return operators


# ─── Data field helpers ─────────────────────────────────────────────────────

def _load_cached_fields(
    dataset_id: str,
    cache_dir: Path = DATAFIELDS_CACHE_DIR,
) -> list[str]:
    """Load all data field IDs for a dataset from the local cache.

    Returns an empty list when the cache does not exist (non-fatal — allows
    limited validation without a prior fetch).
    """
    dataset_dir = cache_dir / dataset_id
    if not dataset_dir.exists():
        return []

    runs = sorted(p for p in dataset_dir.iterdir() if p.is_dir())
    if not runs:
        return []

    latest = runs[-1]
    field_ids: list[str] = []

    for page_file in sorted(latest.glob("page_*.json")):
        try:
            with open(page_file, encoding="utf-8") as fh:
                payload = json.load(fh)
            for result in payload.get("results", []):
                fid = result.get("id")
                if fid:
                    field_ids.append(str(fid))
        except (OSError, json.JSONDecodeError):
            continue

    return field_ids


# ─── Expression parser ──────────────────────────────────────────────────────

_KNOWN_CONSTANTS: set[str] = {
    # Grouping levels
    "market", "sector", "industry", "subindustry",
    # Boolean / sentinel
    "true", "false", "on", "off",
}


def _tokenize(expression: str) -> list[str]:
    """Simple tokenizer: split on punctuation/whitespace boundaries."""
    return re.findall(r"[A-Za-z_][A-Za-z0-9_.]*|\d+\.?\d*|[(),]", expression)


def extract_operator_names(expression: str) -> set[str]:
    """Extract all function-call operator names from an expression."""
    return set(
        m.group(1)
        for m in re.finditer(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(", expression)
    )


def extract_leaf_tokens(expression: str) -> list[str]:
    """Extract leaf tokens — identifiers that are **not** function calls.

    These are candidate data-field references.
    """
    tokens = _tokenize(expression)
    # Naively: anything that looks like an identifier and is immediately
    # followed by "(" is a function call, everything else is a leaf.
    leaves: list[str] = []
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        # Skip parens and commas.
        if tok in ("(", ")", ","):
            i += 1
            continue
        # Check if this token is a function name (followed by "(").
        if i + 1 < len(tokens) and tokens[i + 1] == "(":
            i += 2  # skip function-name and opening paren
            continue
        # Numeric literal.
        if re.match(r"^\d+\.?\d*$", tok):
            i += 1
            continue
        # Known constant.
        if tok.lower() in _KNOWN_CONSTANTS:
            i += 1
            continue
        leaves.append(tok)
        i += 1
    return leaves


# ─── Validation result ──────────────────────────────────────────────────────

class ValidationResult:
    """Structured outcome of an expression validation."""

    def __init__(self):
        self.valid = True
        self.errors: list[str] = []
        self.warnings: list[str] = []

    def add_error(self, msg: str) -> None:
        self.valid = False
        self.errors.append(msg)

    def add_warning(self, msg: str) -> None:
        self.warnings.append(msg)

    def __bool__(self) -> bool:
        return self.valid

    def merge(self, other: "ValidationResult") -> None:
        if not other.valid:
            self.valid = False
        self.errors.extend(other.errors)
        self.warnings.extend(other.warnings)

    @property
    def summary(self) -> str:
        parts = []
        if self.valid:
            parts.append("VALID")
        else:
            parts.append(f"INVALID ({len(self.errors)} error(s))")
        if self.warnings:
            parts.append(f"({len(self.warnings)} warning(s))")
        return " ".join(parts)

    def to_dict(self) -> dict:
        return {
            "valid": self.valid,
            "errors": self.errors,
            "warnings": self.warnings,
        }


# ─── Validator ──────────────────────────────────────────────────────────────

class ExpressionValidator:
    """Validate WQ FASTEXPR expressions against known operators and fields."""

    def __init__(self, operators_path: str | Path = WQ_OPERATORS_FILE):
        self.operators = _load_known_operators(Path(operators_path))
        # Cache loaded fields per dataset.
        self._field_cache: dict[str, set[str]] = {}

    def load_fields(self, dataset_id: str) -> set[str]:
        if dataset_id not in self._field_cache:
            field_ids = _load_cached_fields(dataset_id)
            self._field_cache[dataset_id] = set(field_ids)
        return self._field_cache[dataset_id]

    def validate(
        self,
        expression: str,
        dataset_id: str | None = None,
    ) -> ValidationResult:
        """Run all validation checks on *expression*."""
        result = ValidationResult()

        if not expression or not expression.strip():
            result.add_error("Expression is empty.")
            return result

        # 1. Parenthesis balance.
        depth = 0
        for ch in expression:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            if depth < 0:
                result.add_error("Unmatched closing parenthesis.")
                return result  # short-circuit, further checks meaningless
        if depth != 0:
            result.add_error(f"Unbalanced parentheses (depth={depth} at end).")
            return result

        # 2. Extract and validate operator names.
        operator_names = extract_operator_names(expression)
        if not operator_names:
            result.add_warning("No function calls found — expression has no operators.")

        for op_name in operator_names:
            if op_name not in self.operators:
                result.add_error(f"Unknown operator: '{op_name}'. "
                                 f"Must be one of the 51 WQ operators.")

        # 3. Validate data field references (if dataset context provided).
        if dataset_id:
            known_fields = self.load_fields(dataset_id)
            if known_fields:
                leaf_tokens = extract_leaf_tokens(expression)
                for leaf in leaf_tokens:
                    if leaf not in known_fields:
                        # Could be a valid field not yet cached, or a typo.
                        # Flag as warning, not error — cache may be stale.
                        result.add_warning(
                            f"Field '{leaf}' not found in {dataset_id} cache. "
                            f"Verify it exists or fetch fresh fields."
                        )

        # 4. General well-formedness: no empty parens "()".
        if "()" in expression:
            result.add_warning("Empty parentheses '()' found in expression.")

        return result


# ─── CLI ────────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Validate a WQ FASTEXPR expression.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--expression", required=True, help="WQ expression to validate.")
    parser.add_argument("--dataset-id", default="", help="Check field references against this dataset's cache.")
    parser.add_argument("--json", action="store_true", default=False, help="Output result as JSON.")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    validator = ExpressionValidator()
    dataset_id = args.dataset_id if args.dataset_id else None
    result = validator.validate(args.expression, dataset_id=dataset_id)

    if args.json:
        print(json.dumps(result.to_dict(), ensure_ascii=False, indent=2))
    else:
        print(f"Expression: {args.expression}")
        print(f"Result: {result.summary}")
        for err in result.errors:
            print(f"  \033[91mERROR\033[0m: {err}")
        for warn in result.warnings:
            print(f"  \033[93mWARN\033[0m:  {warn}")

    exit(0 if result.valid else 1)


if __name__ == "__main__":
    main()
