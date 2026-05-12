"""config.py — Shared configuration for the factor-mining system.

All configurable parameters live here.  The ``AgentConfig`` dataclass is used
by the multi-agent orchestrator; module-level constants are used by
``DirectAgent``.
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


# ══════════════════════════════════════════════════════════════════════════
# DirectAgent constants  (used by agent/direct_agent.py)
# ══════════════════════════════════════════════════════════════════════════

# ─── Concurrency & Polling ─────────────────────────────────────────────────
MAX_CONCURRENT = 3
POLL_INTERVAL = 30
MAX_POLL_RETRIES = 3

# ─── Session limits ────────────────────────────────────────────────────────
MAX_ITERATIONS = 1000
TARGET_SHARPE = 2.0
MAX_CONSECUTIVE_STALL = 20
MAX_CONSECUTIVE_STALL_EXPRESSION = 40
MAX_IDLE_POLLS = 60  # 30 min without any completion
MAX_KNOWLEDGE_PER_SESSION = 10
MAX_KNOWLEDGE_PER_ANALYSIS = 2
MAX_CONVERGE_OVERRIDES = 5  # stop overriding after this many times

# ─── FIFO submission queue ─────────────────────────────────────────────────
MAX_QUEUE_RETRIES = 5
MAX_QUEUE_SIZE = 50

# ─── Message truncation limits ─────────────────────────────────────────────
MAX_MSG_CHARS = 200_000
MAX_TOTAL_CHARS = 800_000
TIGHT_MSG_CHARS = 50_000

# ─── Convergence criteria thresholds ───────────────────────────────────────
MIN_SHARPE = 1.25
MIN_FITNESS = 1.0
MIN_TURNOVER = 0.01
MAX_TURNOVER = 0.70

# ─── Phase state machine ───────────────────────────────────────────────────
PHASE_MAX_ROUNDS = 8
PHASE_PROGRESS_STALL = 5

# ─── Phase 0: Idea Discovery ───────────────────────────────────────────────
MAX_DISCOVERY_TURNS = 30
MAX_DISCOVERY_STALL = 5
