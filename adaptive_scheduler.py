"""
adaptive_scheduler.py — Probe-Expand 闭环调度器

职责：
  1. 读取已完成的探测批次（probe batch）回测结果。
  2. 按 core_id 聚合计算代表性指标（Sharpe、Fitness、Turnover）。
  3. 根据可配置的阈值，将每个 Core 分类为：
       - EXPAND  : 代表性参数表现良好，生成全量参数网格批次。
       - WATCH   : 表现平庸，记录但暂不展开，等待更多数据。
       - ABANDON : 表现差，跳过该 Core 的后续搜索。
  4. 对 EXPAND 类 Core，调用 main.py 的生成逻辑产出扩展批次（expand batch）。
  5. 将调度决策写入 JSON 报告，供人工审查或 LLM 进一步分析。

用法示例：
  # 分析探测结果并生成扩展批次
  python adaptive_scheduler.py \\
    --probe-results-dir backtest_results/probe \\
    --template-doc template_catalog.json \\
    --dataset-id option8 \\
    --data-type GROUP \\
    --output-dir factor_batches \\
    --report-file adaptive_scheduler_report.json

  # 仅分析，不生成扩展批次（dry-run）
  python adaptive_scheduler.py \\
    --probe-results-dir backtest_results/probe \\
    --dry-run
"""

import argparse
import json
import subprocess
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path


# ─── 默认阈值 ─────────────────────────────────────────────────────────────────
DEFAULT_EXPAND_MIN_SHARPE = 1.0
DEFAULT_EXPAND_MIN_FITNESS = 1.0
DEFAULT_EXPAND_MAX_TURNOVER = 0.7
DEFAULT_WATCH_MIN_SHARPE = 0.5
DEFAULT_MIN_PROBE_COUNT = 1   # 至少有这么多成功的探测结果才做决策
# ─────────────────────────────────────────────────────────────────────────────

DECISION_EXPAND = "EXPAND"
DECISION_WATCH = "WATCH"
DECISION_ABANDON = "ABANDON"


def extract_metrics(result: dict) -> dict | None:
    """从单条回测结果中提取关键指标，失败时返回 None。"""
    if not isinstance(result, dict) or result.get("status") != "ok":
        return None

    alpha_detail = result.get("alpha_detail", {})
    sim_summary = result.get("simulation_summary", {})

    def _get(d, *keys, default=None):
        for key in keys:
            if isinstance(d, dict) and key in d:
                return d[key]
        return default

    sharpe = _get(alpha_detail, "sharpe", default=_get(sim_summary, "sharpe"))
    fitness = _get(alpha_detail, "fitness", default=_get(sim_summary, "fitness"))
    returns = _get(alpha_detail, "returns", default=_get(sim_summary, "returns"))
    turnover = _get(alpha_detail, "turnover", default=_get(sim_summary, "turnover"))

    if sharpe is None:
        return None

    return {
        "sharpe": float(sharpe),
        "fitness": float(fitness) if fitness is not None else None,
        "returns": float(returns) if returns is not None else None,
        "turnover": float(turnover) if turnover is not None else None,
        "alpha_id": result.get("alpha_id"),
        "regular": result.get("regular", ""),
    }


def load_probe_results(probe_results_dir: Path) -> list[dict]:
    """递归读取探测结果目录下所有 JSON 文件，返回扁平化的因子结果列表。"""
    all_results = []
    for result_file in sorted(probe_results_dir.rglob("*.json")):
        try:
            payload = json.loads(result_file.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            print(f"[WARN] Cannot read {result_file}: {exc}")
            continue

        results = payload.get("results", [])
        if not isinstance(results, list):
            continue

        # 从对应的输入批次文件中读取 core_id 和 template_id（镜像路径关系）
        # 如果结果文件中已经有 core_id（新格式），直接使用；否则尝试从 source_batch 读取
        source_batch_path = payload.get("source_batch", "")
        core_id_map: dict[int, str] = {}
        template_id_map: dict[int, str] = {}

        if source_batch_path:
            try:
                source_payload = json.loads(Path(source_batch_path).read_text(encoding="utf-8"))
                for idx, factor in enumerate(source_payload.get("factors", []), start=1):
                    core_id_map[idx] = factor.get("core_id", "")
                    template_id_map[idx] = factor.get("template_id", "")
            except (OSError, json.JSONDecodeError):
                pass

        for result in results:
            if not isinstance(result, dict):
                continue
            idx = result.get("index", 0)
            # core_id 优先从结果本身取（向后兼容），否则从源批次取
            core_id = result.get("core_id") or core_id_map.get(idx, "")
            template_id = result.get("template_id") or template_id_map.get(idx, "")
            result["core_id"] = core_id
            result["template_id"] = template_id
            result["_source_file"] = str(result_file)
            all_results.append(result)

    return all_results


def aggregate_by_core(results: list[dict]) -> dict[str, dict]:
    """按 core_id 聚合指标，计算均值、最大值等统计量。"""
    core_groups: dict[str, list[dict]] = defaultdict(list)

    for result in results:
        core_id = result.get("core_id", "")
        metrics = extract_metrics(result)
        if metrics is not None:
            metrics["template_id"] = result.get("template_id", "")
            core_groups[core_id].append(metrics)

    aggregated = {}
    for core_id, metric_list in core_groups.items():
        n = len(metric_list)
        sharpes = [m["sharpe"] for m in metric_list]
        fitnesses = [m["fitness"] for m in metric_list if m["fitness"] is not None]
        turnovers = [m["turnover"] for m in metric_list if m["turnover"] is not None]

        aggregated[core_id] = {
            "core_id": core_id,
            "template_id": metric_list[0]["template_id"] if metric_list else "",
            "probe_count": n,
            "sharpe_mean": sum(sharpes) / n,
            "sharpe_max": max(sharpes),
            "sharpe_min": min(sharpes),
            "fitness_mean": sum(fitnesses) / len(fitnesses) if fitnesses else None,
            "turnover_mean": sum(turnovers) / len(turnovers) if turnovers else None,
            "best_alpha_id": max(metric_list, key=lambda m: m["sharpe"])["alpha_id"],
            "best_expression": max(metric_list, key=lambda m: m["sharpe"])["regular"],
            "all_metrics": metric_list,
        }

    return aggregated


def classify_core(
    core_stats: dict,
    expand_min_sharpe: float,
    expand_min_fitness: float,
    expand_max_turnover: float,
    watch_min_sharpe: float,
    min_probe_count: int,
) -> str:
    """根据聚合指标对 Core 做出调度决策。"""
    if core_stats["probe_count"] < min_probe_count:
        return DECISION_WATCH  # 探测数据不足，暂时观察

    sharpe_mean = core_stats["sharpe_mean"]
    fitness_mean = core_stats.get("fitness_mean")
    turnover_mean = core_stats.get("turnover_mean")

    # 检查 Turnover 是否过高（即使 Sharpe 好也不展开）
    if turnover_mean is not None and turnover_mean > expand_max_turnover:
        if sharpe_mean >= watch_min_sharpe:
            return DECISION_WATCH  # Sharpe 尚可但换手率太高，观察
        return DECISION_ABANDON

    # 主要决策逻辑
    fitness_ok = fitness_mean is None or fitness_mean >= expand_min_fitness

    if sharpe_mean >= expand_min_sharpe and fitness_ok:
        return DECISION_EXPAND

    if sharpe_mean >= watch_min_sharpe:
        return DECISION_WATCH

    return DECISION_ABANDON


def generate_expand_batch(
    core_id: str,
    template_id: str,
    dataset_id: str,
    data_type: str,
    template_doc: str,
    output_dir: str,
    slot_overrides_file: str,
    settings_grid_file: str,
    dry_run: bool,
) -> bool:
    """调用 main.py 为指定 Core 生成全量扩展批次（expand batch）。

    通过 slot_overrides 将 core_id 中的字段对固定下来，然后以非 probe 模式
    生成全量参数网格，从而只展开这一个 Core 的所有参数组合。
    """
    # 解析 core_id 为 slot 覆盖配置（格式：slot1=val1|slot2=val2）
    slot_overrides: dict = {"global": {}}
    template_overrides: dict = {}

    for part in core_id.split("|"):
        if "=" in part:
            slot_name, slot_value = part.split("=", 1)
            template_overrides[slot_name.strip()] = [slot_value.strip()]

    if template_overrides:
        slot_overrides[template_id] = template_overrides

    # 写入临时覆盖文件
    overrides_path = Path(output_dir) / f"_expand_override_{template_id}_{datetime.now().strftime('%H%M%S%f')}.json"
    overrides_path.parent.mkdir(parents=True, exist_ok=True)
    overrides_path.write_text(json.dumps(slot_overrides, ensure_ascii=False, indent=2), encoding="utf-8")

    cmd = [
        sys.executable, "main.py",
        "--dataset-id", dataset_id,
        "--data-type", data_type,
        "--template-doc", template_doc,
        "--template-ids", template_id,
        "--slot-overrides-file", str(overrides_path),
        "--output-dir", output_dir,
        # No --probe flag: generate full parameter grid for this core.
    ]
    if settings_grid_file:
        cmd += ["--settings-grid-file", settings_grid_file]

    print(f"  [EXPAND] Core: {core_id}")
    print(f"  Command: {' '.join(cmd)}")

    if dry_run:
        print("  [DRY-RUN] Skipping actual generation.")
        overrides_path.unlink(missing_ok=True)
        return True

    try:
        result = subprocess.run(cmd, check=True, capture_output=False)
        overrides_path.unlink(missing_ok=True)
        return result.returncode == 0
    except subprocess.CalledProcessError as exc:
        print(f"  [ERROR] Generation failed for core {core_id}: {exc}")
        overrides_path.unlink(missing_ok=True)
        return False


def run_scheduler(args: argparse.Namespace) -> None:
    probe_results_dir = Path(args.probe_results_dir)
    if not probe_results_dir.exists():
        raise FileNotFoundError(f"Probe results directory not found: {probe_results_dir}")

    print(f"Loading probe results from: {probe_results_dir}")
    all_results = load_probe_results(probe_results_dir)
    print(f"Loaded {len(all_results)} factor results.")

    if not all_results:
        print("No results found. Exiting.")
        return

    print("Aggregating by core_id...")
    core_stats = aggregate_by_core(all_results)
    print(f"Found {len(core_stats)} unique cores.")

    decisions: dict[str, list] = {DECISION_EXPAND: [], DECISION_WATCH: [], DECISION_ABANDON: []}

    for core_id, stats in sorted(core_stats.items(), key=lambda x: -x[1]["sharpe_mean"]):
        decision = classify_core(
            core_stats=stats,
            expand_min_sharpe=args.expand_min_sharpe,
            expand_min_fitness=args.expand_min_fitness,
            expand_max_turnover=args.expand_max_turnover,
            watch_min_sharpe=args.watch_min_sharpe,
            min_probe_count=args.min_probe_count,
        )
        stats["decision"] = decision
        decisions[decision].append(stats)

    # 打印摘要
    print()
    print("=" * 60)
    print("SCHEDULING DECISIONS SUMMARY")
    print("=" * 60)
    print(f"  EXPAND  : {len(decisions[DECISION_EXPAND])} cores")
    print(f"  WATCH   : {len(decisions[DECISION_WATCH])} cores")
    print(f"  ABANDON : {len(decisions[DECISION_ABANDON])} cores")
    print()

    if decisions[DECISION_EXPAND]:
        print("── EXPAND cores (will generate full parameter grid) ──")
        for stats in decisions[DECISION_EXPAND]:
            print(
                f"  {stats['core_id'][:60]:<60} "
                f"sharpe_mean={stats['sharpe_mean']:.3f}  "
                f"probes={stats['probe_count']}"
            )
        print()

    if decisions[DECISION_WATCH]:
        print("── WATCH cores (borderline, no action yet) ──")
        for stats in decisions[DECISION_WATCH]:
            print(
                f"  {stats['core_id'][:60]:<60} "
                f"sharpe_mean={stats['sharpe_mean']:.3f}  "
                f"probes={stats['probe_count']}"
            )
        print()

    # 生成扩展批次
    expand_success = 0
    expand_fail = 0
    if decisions[DECISION_EXPAND] and not args.no_expand:
        print("── Generating expand batches ──")
        for stats in decisions[DECISION_EXPAND]:
            success = generate_expand_batch(
                core_id=stats["core_id"],
                template_id=stats["template_id"],
                dataset_id=args.dataset_id,
                data_type=args.data_type,
                template_doc=args.template_doc,
                output_dir=args.output_dir,
                slot_overrides_file=args.slot_overrides_file,
                settings_grid_file=args.settings_grid_file,
                dry_run=args.dry_run,
            )
            if success:
                expand_success += 1
            else:
                expand_fail += 1
        print()

    # 写入调度报告
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "probe_results_dir": str(probe_results_dir),
        "thresholds": {
            "expand_min_sharpe": args.expand_min_sharpe,
            "expand_min_fitness": args.expand_min_fitness,
            "expand_max_turnover": args.expand_max_turnover,
            "watch_min_sharpe": args.watch_min_sharpe,
            "min_probe_count": args.min_probe_count,
        },
        "summary": {
            "total_cores": len(core_stats),
            "expand": len(decisions[DECISION_EXPAND]),
            "watch": len(decisions[DECISION_WATCH]),
            "abandon": len(decisions[DECISION_ABANDON]),
            "expand_batches_generated": expand_success,
            "expand_batches_failed": expand_fail,
        },
        "decisions": {
            DECISION_EXPAND: [
                {k: v for k, v in s.items() if k != "all_metrics"}
                for s in decisions[DECISION_EXPAND]
            ],
            DECISION_WATCH: [
                {k: v for k, v in s.items() if k != "all_metrics"}
                for s in decisions[DECISION_WATCH]
            ],
            DECISION_ABANDON: [
                {k: v for k, v in s.items() if k != "all_metrics"}
                for s in decisions[DECISION_ABANDON]
            ],
        },
    }

    report_path = Path(args.report_file)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Scheduling report written to: {report_path}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Probe-Expand adaptive scheduler: classify cores and generate expand batches.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    # ── 输入输出 ──
    parser.add_argument(
        "--probe-results-dir",
        default="backtest_results/probe",
        help="Directory containing backtest results of probe batches.",
    )
    parser.add_argument(
        "--output-dir",
        default="factor_batches",
        help="Directory where expand batch files will be written.",
    )
    parser.add_argument(
        "--report-file",
        default="adaptive_scheduler_report.json",
        help="Path to write the scheduling decision report JSON.",
    )

    # ── 生成参数（传递给 main.py） ──
    parser.add_argument("--dataset-id", default="pv13", help="Dataset id for expand batch generation.")
    parser.add_argument("--data-type", default="GROUP", help="Data field type filter.")
    parser.add_argument("--template-doc", default="template_catalog.json", help="Template catalog JSON file.")
    parser.add_argument("--slot-overrides-file", default="", help="Base slot overrides file (merged with core overrides).")
    parser.add_argument("--settings-grid-file", default="", help="Settings grid file for expand batches.")

    # ── 决策阈值 ──
    parser.add_argument(
        "--expand-min-sharpe",
        type=float,
        default=DEFAULT_EXPAND_MIN_SHARPE,
        help="Minimum mean Sharpe across probe results to trigger EXPAND.",
    )
    parser.add_argument(
        "--expand-min-fitness",
        type=float,
        default=DEFAULT_EXPAND_MIN_FITNESS,
        help="Minimum mean Fitness to trigger EXPAND (ignored if fitness is unavailable).",
    )
    parser.add_argument(
        "--expand-max-turnover",
        type=float,
        default=DEFAULT_EXPAND_MAX_TURNOVER,
        help="Maximum mean Turnover allowed for EXPAND decision.",
    )
    parser.add_argument(
        "--watch-min-sharpe",
        type=float,
        default=DEFAULT_WATCH_MIN_SHARPE,
        help="Minimum mean Sharpe to classify as WATCH instead of ABANDON.",
    )
    parser.add_argument(
        "--min-probe-count",
        type=int,
        default=DEFAULT_MIN_PROBE_COUNT,
        help="Minimum number of successful probe results required before making a decision.",
    )

    # ── 行为控制 ──
    parser.add_argument(
        "--no-expand",
        action="store_true",
        default=False,
        help="Only classify cores and write report; do not generate expand batches.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Print expand commands but do not execute them.",
    )

    args = parser.parse_args()
    run_scheduler(args)


if __name__ == "__main__":
    main()
