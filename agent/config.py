"""config.py — Shared configuration for the multi-agent factor-mining system.

All configurable parameters live here, with sensible defaults that can be
overridden via CLI arguments or environment variables.
"""

from dataclasses import dataclass, field
from typing import Literal


OptimizerMode = Literal["script", "llm"]
SlotAllocation = Literal["1:2", "2:1", "dynamic"]


@dataclass
class AgentConfig:
    """Global configuration for the multi-agent alpha-mining system."""

    # ── Optimisation ──────────────────────────────────────────────────────
    optimizer_mode: OptimizerMode = "script"
    """How to tune expression parameters: ``"script"`` for deterministic grid
    search, ``"llm"`` for LLM-driven parameter suggestions."""

    max_optimizer_rounds: int = 15
    """Maximum WQ submissions per expression during optimisation."""

    # ── Exploration ───────────────────────────────────────────────────────
    explorer_temperature: float = 0.7
    """LLM sampling temperature for the Explorer agent. Higher = more
    structurally diverse (but less reliable) expressions."""

    max_explorer_rounds: int = 10
    """Maximum exploration rounds before the orchestrator stops seeking
    new candidate expressions."""

    explorer_retry_per_round: int = 3
    """How many times the Explorer may retry the LLM if it produces invalid
    or duplicate-structured expressions in one round."""

    # ── Convergence ───────────────────────────────────────────────────────
    target_sharpe: float = 1.5
    """Sharpe ratio above which an expression is considered converged."""

    min_sharpe_for_optimization: float = 0.8
    """Minimum sharpe a candidate expression must reach (with default settings)
    to be passed to the Optimizer."""

    # ── Resource management ───────────────────────────────────────────────
    slot_allocation: SlotAllocation = "1:2"
    """How the 3 WQ concurrent slots are split between Explorer and Optimizer.
    ``"1:2"`` = 1 Explorer slot + 2 Optimiser slots; ``"2:1"`` = reverse;
    ``"dynamic"`` = the Orchestrator adjusts based on progress."""

    # ── Persistence ───────────────────────────────────────────────────────
    reuse_results_db: bool = True
    """Whether to load past results across sessions (avoids retesting)."""

    results_db_path: str = ""
    """Path to the results database JSON file. Empty = use default."""

    # ── Features ──────────────────────────────────────────────────────────
    enable_critic: bool = False
    """Enable the Critic agent (deep analysis + structured knowledge)."""

    verbose: bool = False
    """Print detailed logs during execution."""

    # ── Derived helpers ───────────────────────────────────────────────────

    def explorer_slots(self) -> int:
        if self.slot_allocation == "1:2":
            return 1
        elif self.slot_allocation == "2:1":
            return 2
        return 1  # dynamic → default 1

    def optimizer_slots(self) -> int:
        if self.slot_allocation == "1:2":
            return 2
        elif self.slot_allocation == "2:1":
            return 1
        return 2  # dynamic → default 2

    # ── Factory ───────────────────────────────────────────────────────────

    @classmethod
    def from_cli_args(cls, **kwargs) -> "AgentConfig":
        """Create a config from CLI keyword arguments, falling back to defaults."""
        overrides = {k: v for k, v in kwargs.items() if v is not None and k in cls.__dataclass_fields__}
        return cls(**overrides)

    # ── JSON round-trip ───────────────────────────────────────────────────

    def to_dict(self) -> dict:
        return {
            "optimizer_mode": self.optimizer_mode,
            "explorer_temperature": self.explorer_temperature,
            "max_explorer_rounds": self.max_explorer_rounds,
            "max_optimizer_rounds": self.max_optimizer_rounds,
            "target_sharpe": self.target_sharpe,
            "min_sharpe_for_optimization": self.min_sharpe_for_optimization,
            "slot_allocation": self.slot_allocation,
            "reuse_results_db": self.reuse_results_db,
            "enable_critic": self.enable_critic,
        }
