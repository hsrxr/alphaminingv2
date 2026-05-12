"""
agent/utils.py — Standalone helper utilities for the factor-mining system.
"""


def _find_matching_paren(s: str, start: int) -> int:
    """Return the index of the ')' matching '(' at position *start*."""
    depth = 0
    for i in range(start, len(s)):
        if s[i] == "(":
            depth += 1
        elif s[i] == ")":
            depth -= 1
            if depth == 0:
                return i
    return -1


def _check_named_params(expr: str) -> list[str]:
    """Check for known named-parameter pitfalls in WQ expressions.

    Detects cases like ``winsorize(x, 4)`` that should be ``winsorize(x, std=4)``.
    Handles nested parentheses correctly."""
    warnings: list[str] = []

    # -- winsorize: std= is a named parameter --
    idx = expr.find("winsorize(")
    while idx != -1:
        close = _find_matching_paren(expr, idx + 9)  # len("winsorize(") = 9
        if close == -1:
            break
        args = expr[idx + 9:close]
        if "std=" not in args:
            warnings.append(
                "winsorize(x, std=N) — the std parameter is NAMED, not positional. "
                "Use winsorize(x, std=4) not winsorize(x, 4)"
            )
        idx = expr.find("winsorize(", close)

    # -- ts_regression: rettype= is a named parameter --
    idx = expr.find("ts_regression(")
    while idx != -1:
        close = _find_matching_paren(expr, idx + 14)  # len("ts_regression(") = 14
        if close == -1:
            break
        args = expr[idx + 14:close]
        args_comma = args.count(",")
        if args_comma >= 3 and "rettype" not in args:
            warnings.append(
                "ts_regression(y, x, d, rettype=N) — rettype is a NAMED parameter"
            )
        idx = expr.find("ts_regression(", close)

    # -- group_backfill: std= is a named parameter --
    idx = expr.find("group_backfill(")
    while idx != -1:
        close = _find_matching_paren(expr, idx + 15)  # len("group_backfill(") = 15
        if close == -1:
            break
        args = expr[idx + 15:close]
        args_comma = args.count(",")
        if args_comma >= 3 and "std=" not in args:
            warnings.append(
                "group_backfill(x, group, d, std=N) — std is a NAMED parameter"
            )
        idx = expr.find("group_backfill(", close)

    return warnings


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
