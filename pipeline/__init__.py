"""
pipeline — Factor generation & backtesting pipeline package.

Modules
-------
main                Factor expression generator
backtest_runner     Concurrent backtest execution on WorldQuant Brain
datafields_store    Data-field fetcher and cache
adaptive_scheduler  Probe-Expand adaptive scheduler
result_filter       Post-analysis filtering and aggregation
run_pipeline        Unified one-shot pipeline entry point
"""
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
