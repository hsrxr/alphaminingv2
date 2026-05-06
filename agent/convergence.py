"""
agent/convergence.py — Convergence detection for iterative factor search.

Decides when to stop iterating on a core or the entire search session.
"""

from agent.memory import FactorMemory


# ─── Default thresholds ─────────────────────────────────────────────────────

DEFAULT_MAX_ROUNDS = 5          # Max probe rounds per core
DEFAULT_PATIENCE = 2            # Rounds without meaningful improvement before abandon
DEFAULT_IMPROVEMENT_DELTA = 0.05  # Minimum Sharpe improvement to count as progress
DEFAULT_EXCELLENT_SHARPE = 1.5  # Sharpe above this ⇒ finalize
DEFAULT_MAX_MUTATIONS = 8       # Max mutations attempted on one core
DEFAULT_SESSION_MAX_ROUNDS = 20  # Hard stop for the whole session


class ConvergenceDetector:
    """Decides when to stop iterating.

    Usage::

        detector = ConvergenceDetector()
        status = detector.check_core("core_1", memory)
        # → "continue" | "abandon" | "finalize"

        stop = detector.check_session(memory)
        # → True | False
    """

    def __init__(
        self,
        max_rounds: int = DEFAULT_MAX_ROUNDS,
        patience: int = DEFAULT_PATIENCE,
        improvement_delta: float = DEFAULT_IMPROVEMENT_DELTA,
        excellent_sharpe: float = DEFAULT_EXCELLENT_SHARPE,
        max_mutations: int = DEFAULT_MAX_MUTATIONS,
        session_max_rounds: int = DEFAULT_SESSION_MAX_ROUNDS,
    ):
        self.max_rounds = max_rounds
        self.patience = patience
        self.improvement_delta = improvement_delta
        self.excellent_sharpe = excellent_sharpe
        self.max_mutations = max_mutations
        self.session_max_rounds = session_max_rounds

    # ── core-level ──────────────────────────────────────────────────────

    def check_core(self, core_id: str, memory: FactorMemory) -> str:
        """Evaluate whether to continue, abandon, or finalise a core.

        Returns ``"continue"``, ``"abandon"``, or ``"finalize"``.
        """
        rounds = memory.get_rounds(core_id)
        probe_rounds = [r for r in rounds if r.get("phase") == "probe"]
        decisions = memory.get_decisions(core_id)
        mutations = memory.get_mutations(core_id)

        # 1. Excellent Sharpe ⇒ finalise.
        best_sharpe = memory.get_best_sharpe(core_id)
        if best_sharpe is not None and best_sharpe >= self.excellent_sharpe:
            return "finalize"

        # 2. Too many probe rounds with no improvement ⇒ abandon.
        if len(probe_rounds) >= self.max_rounds:
            improvements = memory.rounds_with_improvement(
                core_id, min_delta=self.improvement_delta
            )
            if len(probe_rounds) - improvements >= self.patience:
                return "abandon"

        # 3. Already marked as EXPAND — let it proceed.
        if decisions and decisions[-1].get("decision") == "EXPAND":
            return "continue"

        # 4. Too many mutation attempts without hitting excellent.
        if len(mutations) >= self.max_mutations:
            return "abandon"

        # 5. Not enough data yet — keep going.
        return "continue"

    # ── session-level ────────────────────────────────────────────────────

    def check_session(self, memory: FactorMemory, current_round: int) -> bool:
        """Return ``True`` when the entire search should stop."""
        if current_round >= self.session_max_rounds:
            return True

        active = memory.get_active_cores()
        if not active:
            # Nothing left to work on.
            return True

        return False

    # ── expand decision ──────────────────────────────────────────────────

    def should_expand(self, core_stats: dict, memory: FactorMemory,
                      core_id: str) -> bool:
        """Rule-based judgment: does this core merit a full expand round?"""
        sharpe = core_stats.get("sharpe_mean", 0) or 0
        turnover = core_stats.get("turnover_mean", 1) or 1
        fitness = core_stats.get("fitness_mean", 0) or 0

        if sharpe >= 0.8 and turnover <= 0.7 and fitness >= 0.3:
            return True

        # Already have a diagnosis that says "promising".
        for d in memory.get_diagnoses(core_id):
            if d.get("diagnosis", {}).get("verdict") == "promising":
                return True

        return False

    def should_mutate(self, core_stats: dict) -> bool:
        """Rule-based: is this core in the mutate-able range?"""
        sharpe = core_stats.get("sharpe_mean", 0) or 0
        return 0.3 <= sharpe < 0.8

    def should_abandon(self, core_stats: dict) -> bool:
        sharpe = core_stats.get("sharpe_mean", 0) or 0
        return sharpe < 0.3
