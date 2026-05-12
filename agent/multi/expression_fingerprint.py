"""expression_fingerprint.py — Structural hashing and distance for WQ expressions.

Used by the Explorer to avoid re-submitting expressions that are structurally
identical or near-identical to previously tested ones.
"""

import re
import hashlib


def _extract_operators(expr: str) -> list[str]:
    """Extract operator names in call order, preserving sequence."""
    return [m.group(1) for m in re.finditer(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(", expr)]


def _extract_fields(expr: str) -> list[str]:
    """Extract leaf tokens that look like data fields (not operators, numbers, constants)."""
    tokens = re.findall(r"[A-Za-z_][A-Za-z0-9_.]*|\d+\.?\d*|[(),]", expr)
    constants = {"market", "sector", "industry", "subindustry",
                 "true", "false", "on", "off"}
    fields: list[str] = []
    i = 0
    while i < len(tokens):
        tok = tokens[i]
        if tok in ("(", ")", ","):
            i += 1
            continue
        if i + 1 < len(tokens) and tokens[i + 1] == "(":
            i += 2
            continue
        if re.match(r"^\d+\.?\d*$", tok):
            i += 1
            continue
        if tok.lower() in constants:
            i += 1
            continue
        fields.append(tok)
        i += 1
    return fields


def expression_fingerprint(expr: str) -> str:
    """Compute a structural hash of an expression.

    The fingerprint is based on the *ordered* set of operators, ignoring
    parameter values and field names. Two expressions with the same sequence
    of operators (e.g. ``ts_decay_linear(ts_scale(X,252),22)`` and
    ``ts_decay_linear(ts_scale(Y,63),10)``) share the same fingerprint.
    """
    operators = _extract_operators(expr)
    if not operators:
        return hashlib.md5(expr.encode()).hexdigest()
    normalized = ",".join(operators)
    return hashlib.md5(normalized.encode()).hexdigest()


def structure_distance(expr_a: str, expr_b: str) -> float:
    """Compute normalised edit distance between two expressions' operator sequences.

    Returns a float in [0, 1] where 0 = identical operator structure and
    1 = completely different operators.
    """
    ops_a = _extract_operators(expr_a)
    ops_b = _extract_operators(expr_b)

    if not ops_a and not ops_b:
        return 0.0
    if not ops_a or not ops_b:
        return 1.0

    # Levenshtein distance on operator sequences.
    m, n = len(ops_a), len(ops_b)
    dp = [[0] * (n + 1) for _ in range(m + 1)]
    for i in range(m + 1):
        dp[i][0] = i
    for j in range(n + 1):
        dp[0][j] = j
    for i in range(1, m + 1):
        for j in range(1, n + 1):
            cost = 0 if ops_a[i - 1] == ops_b[j - 1] else 1
            dp[i][j] = min(dp[i - 1][j] + 1, dp[i][j - 1] + 1, dp[i - 1][j - 1] + cost)

    return dp[m][n] / max(m, n)


def field_fingerprint(expr: str) -> str:
    """Compute a hash based on the *fields* used in an expression.

    Useful for detecting expressions that operate on different data but
    share the same field sources (indicating search-space redundancy).
    """
    fields = sorted(set(_extract_fields(expr)))
    normalized = ",".join(fields)
    return hashlib.md5(normalized.encode()).hexdigest()
