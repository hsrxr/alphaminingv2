"""
result_filter.py — 回测结果过滤与 Core 级别聚合分析

改进内容：
    - 新增 --group-by-core 模式：按 pipeline_core_id 聚合统计，输出 Core 级别的表现摘要
    - collect_result_rows 现在会读取 pipeline_core_id 和 pipeline_template_id 字段（来自新格式的批次文件）
  - 新增 --core-summary-output 参数：将 Core 级别摘要写入 JSON 文件
  - 原有的因子级别过滤功能保持不变
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path


DEFAULT_RESULTS_DIR = "backtest_results"


def _read_json(file_path: Path) -> dict:
    with open(file_path, encoding="utf-8") as file_handle:
        return json.load(file_handle)


def _get_nested_value(data: dict, path: tuple[str, ...]):
    current = data
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return None
        current = current[key]
    return current


def _extract_metric(result_item: dict, metric_name: str):
    metric_paths = {
        "sharpe": [
            ("alpha_detail", "is", "sharpe"),
            ("alpha_detail", "sharpe"),
            ("simulation_summary", "sharpe"),
        ],
        "fitness": [
            ("alpha_detail", "is", "fitness"),
            ("alpha_detail", "fitness"),
            ("simulation_summary", "fitness"),
        ],
        "returns": [
            ("alpha_detail", "is", "returns"),
            ("alpha_detail", "returns"),
            ("simulation_summary", "returns"),
        ],
        "turnover": [
            ("alpha_detail", "is", "turnover"),
            ("alpha_detail", "turnover"),
            ("simulation_summary", "turnover"),
        ],
    }

    for path in metric_paths[metric_name]:
        value = _get_nested_value(result_item, path)
        if isinstance(value, (int, float)):
            return float(value)
    return None


def _load_source_batch_meta(source_batch_path: str) -> dict[int, dict]:
    """从源批次文件中读取 pipeline_core_id / pipeline_template_id 的 index → meta 映射。"""
    if not source_batch_path:
        return {}
    try:
        payload = json.loads(Path(source_batch_path).read_text(encoding="utf-8"))
        meta_map = {}
        for idx, factor in enumerate(payload.get("factors", []), start=1):
            meta_map[idx] = {
                "pipeline_core_id": factor.get("pipeline_core_id") or factor.get("core_id", ""),
                "pipeline_template_id": factor.get("pipeline_template_id") or factor.get("template_id", ""),
            }
        return meta_map
    except (OSError, json.JSONDecodeError):
        return {}


def collect_result_rows(results_dir: Path) -> list[dict]:
    """Load all result files and flatten them into row-like records.

    Improvements over original:
    - Reads pipeline_core_id and pipeline_template_id from result items (new format).
    - Falls back to source_batch file for legacy result files.
    """
    rows = []
    for result_file in sorted(results_dir.rglob("*.json")):
        try:
            payload = _read_json(result_file)
        except (OSError, json.JSONDecodeError) as exc:
            print(f"[WARN] Cannot read {result_file}: {exc}")
            continue

        # Try to load source batch metadata for legacy results that lack pipeline_core_id.
        source_batch_path = payload.get("source_batch", "")
        meta_map = _load_source_batch_meta(source_batch_path)

        for item in payload.get("results", []):
            if not isinstance(item, dict):
                continue

            idx = item.get("index", 0)
            # pipeline_core_id: prefer value in result item, fallback to source batch meta.
            pipeline_core_id = item.get("pipeline_core_id") or item.get("core_id") or meta_map.get(idx, {}).get("pipeline_core_id", "")
            pipeline_template_id = item.get("pipeline_template_id") or item.get("template_id") or meta_map.get(idx, {}).get("pipeline_template_id", "")

            row = {
                "file": str(result_file),
                "regular": item.get("regular", ""),
                "alpha_id": item.get("alpha_id"),
                "status": item.get("status", ""),
                "error": item.get("error", ""),
                "sharpe": _extract_metric(item, "sharpe"),
                "fitness": _extract_metric(item, "fitness"),
                "returns": _extract_metric(item, "returns"),
                "turnover": _extract_metric(item, "turnover"),
                # ── 新增字段 ──
                "pipeline_core_id": pipeline_core_id,
                "pipeline_template_id": pipeline_template_id,
            }
            rows.append(row)
    return rows


def passes_filters(row: dict, args) -> bool:
    """Apply user-provided screening criteria to one result row."""

    if args.status and row.get("status") != args.status:
        return False

    if args.contains and args.contains not in row.get("regular", ""):
        return False

    if args.min_sharpe is not None and (row.get("sharpe") is None or row["sharpe"] < args.min_sharpe):
        return False

    if args.min_fitness is not None and (row.get("fitness") is None or row["fitness"] < args.min_fitness):
        return False

    if args.min_returns is not None and (row.get("returns") is None or row["returns"] < args.min_returns):
        return False

    if args.max_turnover is not None and (row.get("turnover") is None or row["turnover"] > args.max_turnover):
        return False

    return True


# ─────────────────────────────────────────────────────────────────────────────
# 新增：Core 级别聚合分析
# ─────────────────────────────────────────────────────────────────────────────

def build_core_summary(rows: list[dict]) -> list[dict]:
    """Aggregate factor-level rows by pipeline_core_id to produce a Core-level summary.

    For each core, computes:
    - probe_count / total_count: number of factors in the core
    - sharpe_mean / sharpe_max / sharpe_min
    - fitness_mean
    - turnover_mean
    - best_alpha_id / best_expression: the best-performing factor in the core
    - pass_rate: fraction of factors with status == 'ok'
    """
    core_groups: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        core_id = row.get("pipeline_core_id") or row.get("core_id", "")
        core_groups[core_id].append(row)

    summaries = []
    for core_id, group in core_groups.items():
        ok_rows = [r for r in group if r.get("status") == "ok" and r.get("sharpe") is not None]
        n_total = len(group)
        n_ok = len(ok_rows)

        if not ok_rows:
            summaries.append({
                "pipeline_core_id": core_id,
                "pipeline_template_id": (group[0].get("pipeline_template_id") or group[0].get("template_id", "")) if group else "",
                "total_count": n_total,
                "ok_count": n_ok,
                "pass_rate": 0.0,
                "sharpe_mean": None,
                "sharpe_max": None,
                "sharpe_min": None,
                "fitness_mean": None,
                "turnover_mean": None,
                "best_alpha_id": None,
                "best_expression": None,
            })
            continue

        sharpes = [r["sharpe"] for r in ok_rows]
        fitnesses = [r["fitness"] for r in ok_rows if r.get("fitness") is not None]
        turnovers = [r["turnover"] for r in ok_rows if r.get("turnover") is not None]
        best = max(ok_rows, key=lambda r: r["sharpe"])

        summaries.append({
            "pipeline_core_id": core_id,
            "pipeline_template_id": (group[0].get("pipeline_template_id") or group[0].get("template_id", "")) if group else "",
            "total_count": n_total,
            "ok_count": n_ok,
            "pass_rate": round(n_ok / n_total, 4) if n_total else 0.0,
            "sharpe_mean": round(sum(sharpes) / len(sharpes), 4),
            "sharpe_max": round(max(sharpes), 4),
            "sharpe_min": round(min(sharpes), 4),
            "fitness_mean": round(sum(fitnesses) / len(fitnesses), 4) if fitnesses else None,
            "turnover_mean": round(sum(turnovers) / len(turnovers), 4) if turnovers else None,
            "best_alpha_id": best.get("alpha_id"),
            "best_expression": best.get("regular", ""),
        })

    return summaries


def print_core_summary_table(summaries: list[dict], limit: int) -> None:
    """Print a human-readable Core summary table to stdout."""
    sorted_summaries = sorted(
        summaries,
        key=lambda s: float("-inf") if s.get("sharpe_mean") is None else s["sharpe_mean"],
        reverse=True,
    )[:limit]

    header = f"{'pipeline_core_id':<50} {'tpl':<30} {'n':>5} {'sharpe_mean':>12} {'sharpe_max':>10} {'turnover':>9} {'fitness':>8}"
    print()
    print("=" * len(header))
    print("CORE-LEVEL SUMMARY")
    print("=" * len(header))
    print(header)
    print("-" * len(header))
    for s in sorted_summaries:
        sharpe_mean = f"{s['sharpe_mean']:.3f}" if s["sharpe_mean"] is not None else "  N/A"
        sharpe_max = f"{s['sharpe_max']:.3f}" if s["sharpe_max"] is not None else "  N/A"
        turnover = f"{s['turnover_mean']:.3f}" if s["turnover_mean"] is not None else "  N/A"
        fitness = f"{s['fitness_mean']:.3f}" if s["fitness_mean"] is not None else "  N/A"
        core_id_short = (s["pipeline_core_id"] or "(no pipeline_core_id)")[:50]
        tpl_short = (s["pipeline_template_id"] or "")[:30]
        print(
            f"{core_id_short:<50} {tpl_short:<30} {s['ok_count']:>5} "
            f"{sharpe_mean:>12} {sharpe_max:>10} {turnover:>9} {fitness:>8}"
        )
    print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Filter backtest results by user criteria, with optional Core-level aggregation.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--results-dir", default=DEFAULT_RESULTS_DIR, help="Directory containing backtest result files.")
    parser.add_argument("--status", choices=["ok", "failed"], default="ok", help="Filter by execution status.")
    parser.add_argument("--contains", default="", help="Only keep factors whose expression contains this text.")
    parser.add_argument("--min-sharpe", type=float, help="Minimum sharpe threshold.")
    parser.add_argument("--min-fitness", type=float, help="Minimum fitness threshold.")
    parser.add_argument("--min-returns", type=float, help="Minimum returns threshold.")
    parser.add_argument("--max-turnover", type=float, help="Maximum turnover threshold.")
    parser.add_argument("--sort-by", choices=["sharpe", "fitness", "returns", "turnover"], default="sharpe")
    parser.add_argument("--limit", type=int, default=50, help="Maximum number of rows to return.")
    parser.add_argument("--output", default="", help="Optional output JSON file for filtered factor rows.")
    # ── 新增参数 ──
    parser.add_argument(
        "--group-by-core",
        action="store_true",
        default=False,
        help=(
            "Aggregate results by pipeline_core_id and print a Core-level summary table. "
            "Requires factors to have been generated with pipeline_core_id metadata (new main.py format)."
        ),
    )
    parser.add_argument(
        "--core-summary-output",
        default="",
        help="Optional output JSON file for the Core-level summary (used with --group-by-core).",
    )
    args = parser.parse_args()

    results_dir = Path(args.results_dir)
    if not results_dir.exists():
        raise RuntimeError(f"Results directory does not exist: {results_dir}")

    rows = collect_result_rows(results_dir)

    # ── Core 级别聚合模式 ──
    if args.group_by_core:
        summaries = build_core_summary(rows)
        print_core_summary_table(summaries, limit=args.limit)
        print(f"Total cores: {len(summaries)}")

        if args.core_summary_output:
            output_path = Path(args.core_summary_output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(
                json.dumps(sorted(summaries, key=lambda s: -(s["sharpe_mean"] or float("-inf"))),
                           ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            print(f"Core summary saved to: {output_path}")
        return

    # ── 原有因子级别过滤模式 ──
    filtered = [row for row in rows if passes_filters(row, args)]

    reverse = args.sort_by != "turnover"
    filtered.sort(
        key=lambda row: float("-inf") if row.get(args.sort_by) is None else row[args.sort_by],
        reverse=reverse,
    )

    limited = filtered[: max(1, args.limit)]
    print(json.dumps(limited, ensure_ascii=False, indent=2))
    print(f"Matched {len(filtered)} row(s), returned {len(limited)} row(s).")

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as file_handle:
            json.dump(limited, file_handle, ensure_ascii=False, indent=2)
        print(f"Saved filtered rows to: {output_path}")


if __name__ == "__main__":
    main()
