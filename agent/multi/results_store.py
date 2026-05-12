"""results_store.py — Lightweight shared results database for multi-agent system.

Stores all backtest results in a single JSON file so Explorer, Optimizer, and
Orchestrator share a consistent view of what has been tried and what worked.
"""

import json
from pathlib import Path
from datetime import datetime
from threading import Lock


_RESULTS_DB_DEFAULT = Path(__file__).resolve().parent.parent / "agent_output" / "results_db.json"


class ResultsStore:
    """Thread-safe results database backed by a JSON file.

    Each entry records one completed backtest:
      {expression, settings, sharpe, turnover, fitness, alpha_id,
       round, agent, timestamp}
    """

    def __init__(self, path: str | Path = _RESULTS_DB_DEFAULT):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = Lock()
        self._results: list[dict] = self._load()

    # ── public ────────────────────────────────────────────────────────────

    def add_result(
        self,
        expression: str,
        settings: dict | None = None,
        sharpe: float | None = None,
        turnover: float | None = None,
        fitness: float | None = None,
        alpha_id: str = "",
        round_num: int = 0,
        agent: str = "explorer",
    ) -> None:
        """Add one completed backtest result."""
        entry = {
            "expression": expression,
            "settings": settings or {},
            "sharpe": sharpe,
            "turnover": turnover,
            "fitness": fitness,
            "alpha_id": alpha_id,
            "round": round_num,
            "agent": agent,
            "timestamp": datetime.now().isoformat(),
        }
        with self._lock:
            self._results.append(entry)
            self._save()

    def get_best(self, min_sharpe: float = 0.0) -> dict | None:
        """Return the result with the highest sharpe >= min_sharpe."""
        best = None
        with self._lock:
            for r in self._results:
                s = r.get("sharpe")
                if s is not None and s >= min_sharpe:
                    if best is None or s > best.get("sharpe", 0):
                        best = r
        return best

    def get_by_expression(self, expression: str) -> list[dict]:
        """Return all results for a given expression."""
        with self._lock:
            return [r for r in self._results if r["expression"] == expression]

    def get_by_settings(self, expression: str, settings: dict) -> dict | None:
        """Return a specific (expression, settings) result, or None."""
        with self._lock:
            for r in self._results:
                if r["expression"] == expression and r.get("settings") == settings:
                    return r
        return None

    def get_all(self) -> list[dict]:
        """Return all results."""
        with self._lock:
            return list(self._results)

    def get_summary(self) -> dict:
        """Return a summary dict for reporting."""
        with self._lock:
            best = self.get_best()
            by_agent: dict[str, list[dict]] = {}
            for r in self._results:
                by_agent.setdefault(r.get("agent", "unknown"), []).append(r)
            return {
                "total": len(self._results),
                "best_sharpe": best["sharpe"] if best else None,
                "best_expression": best["expression"] if best else None,
                "best_alpha_id": best.get("alpha_id", "") if best else None,
                "by_agent": {k: len(v) for k, v in by_agent.items()},
            }

    def has_been_tested(self, expression: str, settings: dict) -> bool:
        """Check if this exact (expression, settings) pair has been tested."""
        return self.get_by_settings(expression, settings) is not None

    def clear(self) -> None:
        """Clear all results (use with care)."""
        with self._lock:
            self._results.clear()
            self._save()

    # ── internal ──────────────────────────────────────────────────────────

    def _load(self) -> list[dict]:
        try:
            with open(self.path, encoding="utf-8") as fh:
                return json.load(fh)
        except (FileNotFoundError, json.JSONDecodeError):
            return []

    def _save(self) -> None:
        with open(self.path, "w", encoding="utf-8") as fh:
            json.dump(self._results, fh, ensure_ascii=False, indent=2)
