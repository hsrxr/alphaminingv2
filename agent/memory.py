"""
agent/memory.py — Factor iteration memory & knowledge base.

Records every decision, mutation, and backtest result across rounds so the
agent can reason about past attempts and avoid repeating dead ends.
Persists to disk so sessions can be resumed.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Optional


class FactorMemory:
    """Persistent record of the agent's search across rounds.

    Usage::

        memory = FactorMemory("agent_memory")
        memory.init_session("option8", "IV skew predicts returns")
        memory.record_probe_result("core_1", "expr(...)", {"sharpe_mean": 0.85})
        memory.record_decision("core_1", "EXPAND")
        print(memory.summary())

    Resume a past session::

        memory = FactorMemory("agent_memory")
        memory.resume("20260506_123456")
    """

    def __init__(self, storage_dir: str = "agent_memory"):
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)

        # ── serialised state (_.state()) ─────────────────────────────
        self._session_id: str = ""
        self._dataset_id: str = ""
        self._source_idea: str = ""
        self._round: int = 0
        self._cores: dict[str, dict] = {}
        self._mutations_applied: int = 0
        self._llm_calls: int = 0
        self._expand_count: int = 0
        self._abandon_count: int = 0
        self._finalize_count: int = 0

    # ── public properties ───────────────────────────────────────────────

    @property
    def session_id(self) -> str:
        return self._session_id

    @property
    def dataset_id(self) -> str:
        return self._dataset_id

    @property
    def source_idea(self) -> str:
        return self._source_idea

    @property
    def round(self) -> int:
        return self._round

    @property
    def cores(self) -> dict[str, dict]:
        return self._cores

    # ── session lifecycle ───────────────────────────────────────────────

    def init_session(self, dataset_id: str, idea: str) -> None:
        self._session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self._dataset_id = dataset_id
        self._source_idea = idea
        self._round = 0
        self._cores = {}
        self._mutations_applied = 0
        self._llm_calls = 0
        self._expand_count = 0
        self._abandon_count = 0
        self._finalize_count = 0
        self._save()

    def resume(self, session_id: str) -> bool:
        """Load state from a prior session. Returns True on success."""
        self._session_id = session_id
        return self._load()

    # ── event recording ─────────────────────────────────────────────────

    def next_round(self) -> int:
        self._round += 1
        self._save()
        return self._round

    def record_probe_result(
        self, core_id: str, expression: str,
        metrics: dict, template_id: str = "",
    ) -> None:
        self._ensure_core(core_id, expression, template_id)
        self._cores[core_id]["rounds"].append({
            "round": self._round,
            "phase": "probe",
            "metrics": metrics,
            "timestamp": datetime.now().isoformat(timespec="seconds"),
        })
        self._update_best(core_id, expression, metrics)
        self._save()

    def record_expand_result(self, core_id: str, metrics: dict) -> None:
        self._ensure_core(core_id)
        self._cores[core_id]["rounds"].append({
            "round": self._round,
            "phase": "expand",
            "metrics": metrics,
            "timestamp": datetime.now().isoformat(timespec="seconds"),
        })
        self._save()

    def record_diagnosis(self, core_id: str, diagnosis: dict,
                         expression: str = "") -> None:
        self._ensure_core(core_id, expression)
        self._cores[core_id]["diagnoses"].append({
            "round": self._round,
            "diagnosis": diagnosis,
            "timestamp": datetime.now().isoformat(timespec="seconds"),
        })
        self._llm_calls += 1
        self._save()

    def record_decision(self, core_id: str, decision: str,
                        note: str = "") -> None:
        self._ensure_core(core_id)
        self._cores[core_id]["decisions"].append({
            "round": self._round,
            "decision": decision,
            "note": note,
            "timestamp": datetime.now().isoformat(timespec="seconds"),
        })
        counter_map = {"EXPAND": "_expand_count",
                       "ABANDON": "_abandon_count",
                       "FINALIZE": "_finalize_count"}
        attr = counter_map.get(decision)
        if attr:
            setattr(self, attr, getattr(self, attr) + 1)
        self._save()

    def record_mutation(self, core_id: str, original_expr: str,
                        mutation_id: str, mutated_expr: str) -> None:
        self._ensure_core(core_id)
        self._cores[core_id]["mutations"].append({
            "round": self._round,
            "mutation_id": mutation_id,
            "original_expression": original_expr,
            "mutated_expression": mutated_expr,
            "timestamp": datetime.now().isoformat(timespec="seconds"),
        })
        self._mutations_applied += 1
        self._save()

    # ── queries ─────────────────────────────────────────────────────────

    def get_rounds(self, core_id: str) -> list[dict]:
        return self._cores.get(core_id, {}).get("rounds", [])

    def get_diagnoses(self, core_id: str) -> list[dict]:
        return self._cores.get(core_id, {}).get("diagnoses", [])

    def get_decisions(self, core_id: str) -> list[dict]:
        return self._cores.get(core_id, {}).get("decisions", [])

    def get_mutations(self, core_id: str) -> list[dict]:
        return self._cores.get(core_id, {}).get("mutations", [])

    def get_best_sharpe(self, core_id: str) -> float:
        return self._cores.get(core_id, {}).get("best_sharpe", float("-inf"))

    def get_decision_count(self) -> int:
        return sum(len(c["decisions"]) for c in self._cores.values())

    def get_active_cores(self) -> list[tuple[str, dict]]:
        """Cores not yet abandoned or finalized."""
        terminal = {"ABANDON", "FINALIZE"}
        results: list[tuple[str, dict]] = []
        for cid, core in self._cores.items():
            decs = core.get("decisions", [])
            if decs and decs[-1]["decision"] in terminal:
                continue
            results.append((cid, core))
        return results

    def rounds_with_improvement(
        self, core_id: str, min_delta: float = 0.05
    ) -> int:
        """Count how many probe rounds showed meaningful Sharpe improvement."""
        rounds = [
            r for r in self._cores.get(core_id, {}).get("rounds", [])
            if r.get("phase") == "probe"
        ]
        if len(rounds) < 2:
            return 0
        improvements = 0
        for i in range(1, len(rounds)):
            prev_s = rounds[i - 1].get("metrics", {}).get("sharpe_mean", 0) or 0
            curr_s = rounds[i].get("metrics", {}).get("sharpe_mean", 0) or 0
            if curr_s - prev_s >= min_delta:
                improvements += 1
        return improvements

    # ── reporting ───────────────────────────────────────────────────────

    def summary(self) -> dict:
        return {
            "session_id": self._session_id,
            "dataset_id": self._dataset_id,
            "source_idea": self._source_idea,
            "total_rounds": self._round,
            "total_cores": len(self._cores),
            "mutations_applied": self._mutations_applied,
            "llm_calls": self._llm_calls,
            "expansions": self._expand_count,
            "abandons": self._abandon_count,
            "finalized": self._finalize_count,
            "active_cores": len(self.get_active_cores()),
            "cores": {
                cid: {
                    "best_sharpe": c.get("best_sharpe"),
                    "template_id": c.get("template_id", ""),
                    "expression": (c.get("best_expression", "") or "")[:120],
                    "rounds": len(c["rounds"]),
                    "decisions": [d["decision"] for d in c["decisions"]],
                    "mutations": [m["mutation_id"] for m in c["mutations"]],
                }
                for cid, c in sorted(self._cores.items())
            },
        }

    # ── internals ───────────────────────────────────────────────────────

    def _ensure_core(self, core_id: str, expression: str = "",
                     template_id: str = "") -> None:
        if core_id not in self._cores:
            self._cores[core_id] = {
                "core_id": core_id,
                "template_id": template_id,
                "expression": expression,
                "best_sharpe": float("-inf"),
                "best_expression": "",
                "rounds": [],
                "diagnoses": [],
                "decisions": [],
                "mutations": [],
            }

    def _update_best(self, core_id: str, expression: str,
                     metrics: dict) -> None:
        new_sharpe = metrics.get("sharpe_mean", 0)
        if new_sharpe is not None and new_sharpe > self._cores[core_id].get("best_sharpe", float("-inf")):
            self._cores[core_id]["best_sharpe"] = new_sharpe
            self._cores[core_id]["best_expression"] = expression

    def _state(self) -> dict:
        """Serialisable state snapshot."""
        return {
            "_session_id": self._session_id,
            "_dataset_id": self._dataset_id,
            "_source_idea": self._source_idea,
            "_round": self._round,
            "_cores": self._cores,
            "_mutations_applied": self._mutations_applied,
            "_llm_calls": self._llm_calls,
            "_expand_count": self._expand_count,
            "_abandon_count": self._abandon_count,
            "_finalize_count": self._finalize_count,
        }

    def _state_path(self) -> Path:
        return self.storage_dir / f"session_{self._session_id}.json"

    def _load(self) -> bool:
        path = self._state_path()
        if not path.exists():
            return False
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            self.__dict__.update(data)
            return True
        except (OSError, json.JSONDecodeError):
            return False

    def _save(self) -> None:
        if not self._session_id:
            return
        path = self._state_path()
        path.write_text(
            json.dumps(self._state(), ensure_ascii=False, indent=2, default=str),
            encoding="utf-8",
        )
