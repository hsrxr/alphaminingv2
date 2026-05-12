#!/usr/bin/env python3
"""multi_agent.py — CLI entry point for the multi-agent alpha mining system.

Usage::

    # Run with investment thesis (uses Explorer + Optimizer)
    python -m agent.multi_agent --idea "Companies with high FCF outperform"

    # Run with existing expression (skips Explorer, goes straight to Optimizer)
    python -m agent.multi_agent --expression "ts_decay_linear(ts_scale(est_cashflow_op,252),22) - ts_decay_linear(ts_scale(est_capex,252),22)"

    # Run with script optimizer (default) vs LLM optimizer
    python -m agent.multi_agent --idea "..." --optimizer-mode script
    python -m agent.multi_agent --idea "..." --optimizer-mode llm

    # Run with auto idea discovery (no idea needed)
    python -m agent.multi_agent --auto-discover

    # Original DirectAgent still available
    python -m agent.direct_agent --idea "..."
"""

import argparse
import sys
from pathlib import Path

# Ensure the project root is on sys.path.
_project_root = Path(__file__).resolve().parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))

from agent.config import AgentConfig
from agent.multi.orchestrator import OrchestratorV2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Multi-agent alpha factor mining system.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--idea", default="",
                        help="Investment thesis / factor idea.")
    parser.add_argument("--expression", default="",
                        help="Existing WQ expression to start from (skips Explorer).")
    parser.add_argument("--auto-discover", action="store_true",
                        help="Automatically discover ideas via web search (no --idea needed).")
    parser.add_argument("--optimizer-mode", choices=["script", "llm"], default="script",
                        help="Optimizer strategy: script (grid search) or llm.")
    parser.add_argument("--explorer-temperature", type=float, default=0.7,
                        help="LLM temperature for the Explorer (higher = more diverse).")
    parser.add_argument("--target-sharpe", type=float, default=1.5,
                        help="Target Sharpe ratio for convergence.")
    parser.add_argument("--output-dir", default="agent_output",
                        help="Output directory for session data.")
    parser.add_argument("--dataset-id", default="",
                        help="WQ dataset ID (optional).")
    parser.add_argument("--verbose", action="store_true",
                        help="Enable verbose logging.")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    config = AgentConfig(
        optimizer_mode=args.optimizer_mode,
        explorer_temperature=args.explorer_temperature,
        target_sharpe=args.target_sharpe,
        verbose=args.verbose,
    )

    # If auto-discover, run IdeaScout first to generate the thesis.
    idea = args.idea
    auto_discover = args.auto_discover

    if not idea and not args.expression and auto_discover:
        print("Auto-discovery mode: searching for factor ideas...")
        from agent.multi.idea_scout import IdeaScout
        from agent.wq_tools import WQTools
        from agent.llm_client import LLMClient
        from datetime import datetime
        tools = WQTools()
        tools.login()
        llm = LLMClient()
        scout_log_dir = Path(args.output_dir) / f"scout_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        scout = IdeaScout(tools, llm, log_dir=scout_log_dir)
        ideas = scout.discover(dataset_id=args.dataset_id)
        if ideas:
            idea = ideas[0].get("hypothesis", "")
            print(f"Discovered idea: {idea[:120]}...")
        else:
            print("Idea discovery failed. Provide an --idea manually.")
            sys.exit(1)

    orch = OrchestratorV2(
        config=config,
        output_dir=args.output_dir,
        dataset_id=args.dataset_id,
    )
    report = orch.run(idea=idea, expression=args.expression)

    # Print summary.
    best = report.get("best_sharpe")
    total = report.get("total_submissions", 0)
    print(f"\n{'='*50}")
    print(f"Session: {report.get('session_id', '?')}")
    print(f"Best Sharpe: {best or 'N/A'}")
    print(f"Total submissions: {total}")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
