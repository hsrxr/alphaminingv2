"""Phase state machine — Creative ↔ Improvement switching and direction management."""

from agent.config import MAX_CONVERGE_OVERRIDES, PHASE_MAX_ROUNDS, PHASE_PROGRESS_STALL


class PhaseManager:
    """Manages the creative/improvement phase alternation.

    Responsibilities:
    - Creative ↔ Improvement switching
    - Direction creation, tracking, and abandonment
    - Phase transition evaluation (abandon-all, max-rounds, stalled progress)
    - Converge override flag signalling
    """

    def __init__(self, quiet: bool, log_event):
        self.quiet = quiet
        self._log_event = log_event

        # Phase state.
        self._phase: str = "creative"
        self._phase_rounds: int = 0
        self._phase_progress_streak: int = 0
        self._best_sharpe_at_phase_start: float = 0.0

        # Directions.
        self._directions: list[dict] = []
        self._all_directions_history: list[dict] = []
        self._direction_rounds: dict[str, int] = {}

        # Guards.
        self._phase_reentry_guard: bool = False
        self._converge_override_triggered: bool = False
        self._converge_override_count: int = 0

    # ── Properties ─────────────────────────────────────────────────────────

    @property
    def phase(self) -> str:
        return self._phase

    @property
    def phase_rounds(self) -> int:
        return self._phase_rounds

    @property
    def directions(self) -> list[dict]:
        return self._directions

    @property
    def all_directions_history(self) -> list[dict]:
        return self._all_directions_history

    @property
    def converge_override_triggered(self) -> bool:
        return self._converge_override_triggered

    @converge_override_triggered.setter
    def converge_override_triggered(self, val: bool) -> None:
        self._converge_override_triggered = val

    @property
    def converge_override_count(self) -> int:
        return self._converge_override_count

    def set_converge_override(self) -> bool:
        """Mark a converge override and return True if still allowed.

        Returns False when the override limit has been reached — the
        caller should force-converge instead.
        """
        self._converge_override_count += 1
        self._converge_override_triggered = True
        return self._converge_override_count <= MAX_CONVERGE_OVERRIDES

    @property
    def reentry_guard(self) -> bool:
        return self._phase_reentry_guard

    @reentry_guard.setter
    def reentry_guard(self, val: bool) -> None:
        self._phase_reentry_guard = val

    # ── Phase switching ────────────────────────────────────────────────────

    def set_initial_phase(self) -> None:
        """Start iteration in creative phase for initial direction generation."""
        self._phase = "creative"
        self._phase_rounds = 0
        self._phase_progress_streak = 0
        self._best_sharpe_at_phase_start = 0.0
        self._directions = []
        self._phase_reentry_guard = False
        self._log_event("phase_start", phase="creative")

    def switch_to_improvement(self, best_sharpe_fn) -> None:
        """Transition to improvement phase."""
        if not self._directions:
            return
        self._phase = "improve"
        self._phase_rounds = 0
        self._phase_progress_streak = 0
        self._best_sharpe_at_phase_start = best_sharpe_fn()
        active = [d for d in self._directions if not d.get("abandoned")]
        self._log_event(
            "phase_switch", phase="improve",
            directions=[d["direction_id"] for d in active],
        )

    def switch_to_creative(self, best_sharpe_fn) -> None:
        """Transition to creative phase for fresh hypothesis generation."""
        self._phase = "creative"
        self._phase_rounds = 0
        self._phase_progress_streak = 0
        self._best_sharpe_at_phase_start = best_sharpe_fn()
        self._log_event("phase_switch", phase="creative")

    def check_phase_transition(self, current_best_sharpe: float) -> bool:
        """Evaluate whether the current phase should end.

        Returns ``True`` when a phase switch occurred (the caller should
        re-prompt the LLM in the new phase).
        """
        if self._phase == "creative":
            return False

        # 1. All directions abandoned?
        active = [d for d in self._directions if not d.get("abandoned")]
        if not active:
            if not self.quiet:
                print("  [PHASE SWITCH] All directions abandoned → CREATIVE")
            self.switch_to_creative(lambda: current_best_sharpe)
            return True

        # 2. Update progress streak.
        if current_best_sharpe > self._best_sharpe_at_phase_start:
            self._phase_progress_streak = 0
            self._best_sharpe_at_phase_start = current_best_sharpe
        else:
            self._phase_progress_streak += 1

        # 3. Max rounds reached?
        if self._phase_rounds >= PHASE_MAX_ROUNDS:
            if not self.quiet:
                print(f"  [PHASE SWITCH] {self._phase_rounds} improvement rounds reached → CREATIVE")
            self.switch_to_creative(lambda: current_best_sharpe)
            return True

        # 4. Stalled progress?
        if self._phase_progress_streak >= PHASE_PROGRESS_STALL:
            if not self.quiet:
                print(f"  [PHASE SWITCH] No improvement for {self._phase_progress_streak} rounds → CREATIVE")
            self.switch_to_creative(lambda: current_best_sharpe)
            return True

        return False

    # ── Direction management ───────────────────────────────────────────────

    def set_directions(self, raw_directions: list[dict], iteration: int) -> None:
        """Persist new directions (wipes old, replaces with fresh batch)."""
        self._directions = []
        for i, d in enumerate(raw_directions):
            did = d.get("direction_id", "") or f"d{i+1}"
            entry = {
                "direction_id": did,
                "hypothesis": d.get("hypothesis", ""),
                "target_check": d.get("target_check", ""),
                "approach": d.get("approach", ""),
                "rounds": 0,
                "best_sharpe": 0.0,
                "improvement_count": 0,
                "abandoned": False,
                "created_at_round": iteration,
            }
            self._directions.append(entry)
            self._all_directions_history.append(entry)

        self._direction_rounds = {d["direction_id"]: 0 for d in self._directions}

    def track_improvement(self, direction_id: str) -> None:
        """Increment round/improvement counters for a direction."""
        self._direction_rounds[direction_id] = self._direction_rounds.get(direction_id, 0) + 1
        for d in self._directions:
            if d["direction_id"] == direction_id:
                d["rounds"] += 1
                d["improvement_count"] = d.get("improvement_count", 0) + 1
                break

    def update_best_sharpe(self, direction_id: str, sharpe: float, round_num: int = 0) -> None:
        """Update the best sharpe for a direction (called from polling)."""
        for d in self._directions:
            if d["direction_id"] == direction_id and sharpe > d.get("best_sharpe", 0):
                d["best_sharpe"] = sharpe
                d["rounds"] = max(d["rounds"], round_num)
                break

    def get_active_direction_ids(self) -> set[str]:
        return {d["direction_id"] for d in self._directions if not d.get("abandoned")}

    # ── Abandonment ────────────────────────────────────────────────────────

    def collect_abandon_requests(self, response: dict) -> set[str]:
        """Extract direction IDs that the LLM wants to abandon.

        Returns a set of candidate direction_ids (before guard checks).
        """
        ids: set[str] = set()
        if response.get("abandoned_directions"):
            ids.update(response["abandoned_directions"])
        if response.get("abandoned"):
            # Legacy field: abandon ALL active directions.
            ids.update(
                d["direction_id"] for d in self._directions if not d.get("abandoned")
            )
        return ids

    def filter_abandon_requests(self, abandon_ids: set[str]) -> tuple[set[str], list[str]]:
        """Apply the 3-round minimum guard.

        Returns ``(approved_ids, blocked_ids)``.
        """
        blocked = [did for did in abandon_ids if self._direction_rounds.get(did, 0) < 3]
        approved = abandon_ids - set(blocked)

        for did in approved:
            for d in self._directions:
                if d["direction_id"] == did and not d.get("abandoned"):
                    d["abandoned"] = True
                    self._log_event(
                        "direction_abandoned",
                        direction_id=did, rounds=d["rounds"],
                        best_sharpe=d["best_sharpe"],
                    )
                    break

        return approved, blocked

    def is_all_abandoned(self) -> bool:
        return not any(not d.get("abandoned") for d in self._directions)

    # ── Validation ─────────────────────────────────────────────────────────

    def validate_improvement_direction_ids(self, improvements: list[dict]) -> tuple[list[dict], list[str]]:
        """Filter improvements to only those with active direction IDs.

        Returns ``(valid_improvements, warnings)``.
        """
        active_ids = self.get_active_direction_ids()
        valid = []
        warnings = []
        for imp in improvements:
            did = imp.get("direction_id", "")
            if did and did not in active_ids:
                w = (
                    f"WARNING: direction_id='{did}' is not active. "
                    f"Active directions: {sorted(active_ids)}. Skipping."
                )
                warnings.append(w)
                continue
            valid.append(imp)
        return valid, warnings
