"""
agent/cli.py — Command-line entry point for the DirectAgent factor miner.
"""

import argparse
import json
import sys
import time

from agent.config import MAX_ITERATIONS, TARGET_SHARPE
from agent.direct.agent import DirectAgent
from agent.llm_client import load_api_key


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Direct agent for autonomous factor mining on WorldQuant Brain.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--dataset-id", default="",
                        help="Target dataset (e.g. pv13, option8). Omit to let agent choose.")
    parser.add_argument("--idea", default="", help="Financial idea in plain English.")
    parser.add_argument("--expression", default="", help="Existing expression to improve.")
    parser.add_argument("--critique", default="", help="Targeted critique / improvement goal.")
    parser.add_argument("--settings", default="",
                        help='JSON settings, e.g. \'{"neutralization":"INDUSTRY","decay":10}\'. '
                             'Overrides Brain API defaults for the baseline and shown as a hint to the LLM.')
    parser.add_argument("--neutralization", default="",
                        choices=["MARKET", "INDUSTRY", "SECTOR", "SUBINDUSTRY", "NONE"],
                        help="Neutralization level for the baseline expression.")
    parser.add_argument("--decay", type=int, default=None,
                        help="Decay (half-life in days, e.g. 3/5/10) for the baseline expression. "
                             "Omit = use Brain default.")
    parser.add_argument("--delay", type=int, default=None, choices=[0, 1],
                        help="Delay (0 or 1) for the baseline expression. Omit = use Brain default.")
    parser.add_argument("--truncation", type=float, default=None,
                        help="Truncation limit (0.00-1.00) for the baseline expression. "
                             "Omit = use Brain default.")
    parser.add_argument("--iterations", type=int, default=MAX_ITERATIONS,
                        help="Max analysis rounds.")
    parser.add_argument("--target-sharpe", type=float, default=TARGET_SHARPE,
                        help="Convergence target.")
    parser.add_argument("--output-dir", default="agent_output", help="Output directory.")
    parser.add_argument("--model", default="deepseek-chat", help="LLM model name.")
    parser.add_argument("--quiet", action="store_true", default=False)
    return parser


def main() -> None:
    sys.stdout.reconfigure(encoding="utf-8")
    parser = build_parser()
    args = parser.parse_args()

    if not args.idea and not args.expression:
        if not args.quiet:
            print("No --idea or --expression provided. Will attempt autonomous idea discovery (Phase 0).")

    api_key = load_api_key()

    user_settings = {}
    if args.settings:
        try:
            user_settings = json.loads(args.settings)
        except json.JSONDecodeError as e:
            parser.error(f"Invalid --settings JSON: {e}")

    if args.neutralization:
        user_settings["neutralization"] = args.neutralization
    if args.decay is not None:
        user_settings["decay"] = args.decay
    if args.delay is not None:
        user_settings["delay"] = args.delay
    if args.truncation is not None:
        user_settings["truncation"] = args.truncation

    agent = DirectAgent(
        dataset_id=args.dataset_id,
        api_key=api_key,
        model=args.model,
        output_dir=args.output_dir,
        quiet=args.quiet,
        max_iterations=args.iterations,
        target_sharpe=args.target_sharpe,
    )

    t0 = time.time()
    try:
        summary = agent.run(
            idea=args.idea,
            expression=args.expression,
            critique=args.critique,
            settings=user_settings,
        )
    except Exception as exc:
        print(f"\n[FATAL] Agent crashed: {exc}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        summary = agent._final_report()
    elapsed = time.time() - t0
    print(f"\nTotal time: {elapsed:.1f}s")
    if summary.get("error"):
        sys.exit(1)


if __name__ == "__main__":
    main()
