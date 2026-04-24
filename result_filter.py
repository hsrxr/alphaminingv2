import argparse
import json
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


def collect_result_rows(results_dir: Path) -> list[dict]:
    """Load all result files and flatten them into row-like records."""

    rows = []
    for result_file in sorted(results_dir.rglob("*.json")):
        payload = _read_json(result_file)
        for item in payload.get("results", []):
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Filter backtest results by user criteria.")
    parser.add_argument("--results-dir", default=DEFAULT_RESULTS_DIR, help="Directory containing backtest result files.")
    parser.add_argument("--status", choices=["ok", "failed"], default="ok", help="Filter by execution status.")
    parser.add_argument("--contains", default="", help="Only keep factors whose expression contains this text.")
    parser.add_argument("--min-sharpe", type=float, help="Minimum sharpe threshold.")
    parser.add_argument("--min-fitness", type=float, help="Minimum fitness threshold.")
    parser.add_argument("--min-returns", type=float, help="Minimum returns threshold.")
    parser.add_argument("--max-turnover", type=float, help="Maximum turnover threshold.")
    parser.add_argument("--sort-by", choices=["sharpe", "fitness", "returns", "turnover"], default="sharpe")
    parser.add_argument("--limit", type=int, default=50, help="Maximum number of rows to return.")
    parser.add_argument("--output", default="", help="Optional output JSON file for filtered rows.")
    args = parser.parse_args()

    results_dir = Path(args.results_dir)
    if not results_dir.exists():
        raise RuntimeError(f"Results directory does not exist: {results_dir}")

    rows = collect_result_rows(results_dir)
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
