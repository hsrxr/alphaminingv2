"""
agent — Alpha factor search agent package.

Modules
-------
llm_client              DeepSeek/LLM API client
expression_validator    WQ expression syntax & field validation
result_analyzer         Deep analysis of backtest results using LLM
template_generator      Generate templates from financial ideas
mutation_engine         Factor mutation from diagnostic analysis
feedback_loop           Interactive agent loop (Phase 1 entry point)
memory                  Factor iteration memory & knowledge base
convergence             Convergence detection for iterative search
orchestrator            Full autonomous factor search loop (Phase 3)
"""
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
