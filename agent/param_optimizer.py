"""
@deprecated: 已迁移至 agent.multi.param_optimizer
请使用: from agent.multi.param_optimizer import ParamOptimizer
"""

from agent.multi.param_optimizer import (  # noqa: F401
    ParamOptimizer,
    LLMOptimizer,
    OptimizerSuggestion,
)
