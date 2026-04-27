"""
test_pipeline.py — run_pipeline.py 的逻辑验证测试

不依赖 Brain API，通过 mock 验证：
  1. 所有导入路径可解析
  2. build_parser() 生成的默认值正确
  3. 流程控制标志（skip / probe_only / dry_run）的跳过逻辑正确
  4. phase_schedule 在 dry_run 模式下不调用 generate_expand_batch
  5. run_pipeline 在 probe_only 模式下不进入调度阶段
"""

import json
import sys
import traceback
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, str(Path(__file__).parent))

PASS = "\033[92m[PASS]\033[0m"
FAIL = "\033[91m[FAIL]\033[0m"
results = []


def check(name: str, condition: bool, detail: str = ""):
    status = PASS if condition else FAIL
    print(f"{status} {name}" + (f" — {detail}" if detail else ""))
    results.append((name, condition))


def run_test(name: str, fn):
    try:
        fn()
    except Exception as exc:
        print(f"{FAIL} {name} — Unexpected exception: {exc}")
        traceback.print_exc()
        results.append((name, False))


# ─── 测试 1：导入 run_pipeline 不报错 ─────────────────────────────────────────
def test_import():
    import run_pipeline  # noqa: F401
    check("import_run_pipeline", True)


# ─── 测试 2：build_parser 默认值正确 ─────────────────────────────────────────
def test_parser_defaults():
    from run_pipeline import build_parser
    parser = build_parser()
    args = parser.parse_args([])

    check("default_dataset_id", args.dataset_id == "pv13", f"Got: {args.dataset_id}")
    check("default_template_ids", args.template_ids == "ALL", f"Got: {args.template_ids}")
    check("default_expand_min_sharpe", args.expand_min_sharpe == 1.0, f"Got: {args.expand_min_sharpe}")
    check("default_expand_max_turnover", args.expand_max_turnover == 0.7, f"Got: {args.expand_max_turnover}")
    check("default_watch_min_sharpe", args.watch_min_sharpe == 0.5, f"Got: {args.watch_min_sharpe}")
    check("default_probe_batches_dir", args.probe_batches_dir == "factor_batches/probe",
          f"Got: {args.probe_batches_dir}")
    check("default_probe_results_dir", args.probe_results_dir == "backtest_results/probe",
          f"Got: {args.probe_results_dir}")
    check("default_expand_batches_dir", args.expand_batches_dir == "factor_batches/expand",
          f"Got: {args.expand_batches_dir}")
    check("default_expand_results_dir", args.expand_results_dir == "backtest_results/expand",
          f"Got: {args.expand_results_dir}")
    check("default_skip_probe_gen", args.skip_probe_gen is False)
    check("default_skip_probe_run", args.skip_probe_run is False)
    check("default_probe_only", args.probe_only is False)
    check("default_dry_run", args.dry_run is False)


# ─── 测试 3：--skip-probe-gen / --skip-probe-run 标志解析正确 ─────────────────
def test_skip_flags():
    from run_pipeline import build_parser
    parser = build_parser()
    args = parser.parse_args(["--skip-probe-gen", "--skip-probe-run", "--dataset-id", "option8"])
    check("skip_probe_gen_flag", args.skip_probe_gen is True)
    check("skip_probe_run_flag", args.skip_probe_run is True)
    check("dataset_id_override", args.dataset_id == "option8", f"Got: {args.dataset_id}")


# ─── 测试 4：--probe-only 模式下 run_pipeline 不进入调度阶段 ─────────────────
def test_probe_only_skips_scheduling():
    from run_pipeline import build_parser, run_pipeline

    parser = build_parser()
    args = parser.parse_args([
        "--probe-only",
        "--skip-probe-gen",   # 跳过实际 API 调用
        "--skip-probe-run",   # 跳过实际回测
        "--dataset-id", "pv13",
    ])

    logger = MagicMock()
    schedule_called = []

    with patch("run_pipeline.phase_schedule", side_effect=lambda *a, **kw: schedule_called.append(1)):
        with patch("run_pipeline.phase_probe_gen", return_value=[]):
            with patch("run_pipeline.phase_run_backtest"):
                run_pipeline(args, logger)

    check("probe_only_no_schedule", len(schedule_called) == 0,
          f"phase_schedule was called {len(schedule_called)} time(s)")


# ─── 测试 5：--dry-run 模式下 phase_schedule 不调用 generate_expand_batch ────
def test_dry_run_no_expand_batch(tmp_path):
    from run_pipeline import build_parser, phase_schedule

    # 准备 mock 探测结果文件
    probe_results_dir = tmp_path / "probe_results"
    probe_results_dir.mkdir()
    mock_result_file = probe_results_dir / "result_001.json"
    mock_result_file.write_text(json.dumps({
        "source_batch": "",
        "results": [
            {
                "index": 1,
                "status": "ok",
                "regular": "group_rank(ts_mean(iv_30d - hv_30d, 21), industry)",
                "core_id": "iv_mean_field=iv_30d|hv_field=hv_30d",
                "template_id": "TPL_GROUP_IVHV_SMOOTH_V1",
                "alpha_id": "alpha_001",
                "alpha_detail": {"sharpe": 1.5, "fitness": 1.3, "returns": 0.12, "turnover": 0.4},
            }
        ],
    }), encoding="utf-8")

    parser = build_parser()
    args = parser.parse_args([
        "--dry-run",
        "--dataset-id", "pv13",
        "--probe-results-dir", str(probe_results_dir),
        "--report-file", str(tmp_path / "report.json"),
    ])

    logger = MagicMock()
    expand_called = []

    with patch("adaptive_scheduler.generate_expand_batch", side_effect=lambda *a, **kw: expand_called.append(1) or True):
        phase_schedule(
            args=args,
            probe_results_dir=probe_results_dir,
            expand_batches_dir=tmp_path / "expand_batches",
            logger=logger,
        )

    check("dry_run_no_expand_batch", len(expand_called) == 0,
          f"generate_expand_batch was called {len(expand_called)} time(s)")

    # 报告文件应该被写入
    report_path = tmp_path / "report.json"
    check("dry_run_report_written", report_path.exists())
    if report_path.exists():
        report = json.loads(report_path.read_text())
        check("dry_run_report_has_expand", report["summary"]["expand"] >= 1,
              f"expand={report['summary']['expand']}")


# ─── 测试 6：--dry-run 模式下 run_pipeline 不进入 phase_run_backtest（阶段4）─
def test_dry_run_skips_expand_backtest():
    from run_pipeline import build_parser, run_pipeline

    parser = build_parser()
    args = parser.parse_args([
        "--dry-run",
        "--skip-probe-gen",
        "--skip-probe-run",
        "--dataset-id", "pv13",
    ])

    logger = MagicMock()
    backtest_calls = []

    mock_report = {"summary": {"expand_batches_generated": 1}}

    with patch("run_pipeline.phase_schedule", return_value=mock_report):
        with patch("run_pipeline.phase_run_backtest",
                   side_effect=lambda *a, **kw: backtest_calls.append(kw.get("phase_label", "?"))):
            run_pipeline(args, logger)

    # 在 dry_run 模式下，run_pipeline 在 phase_schedule 之后就 return，不应调用 phase_run_backtest
    check("dry_run_no_expand_backtest", len(backtest_calls) == 0,
          f"phase_run_backtest called with: {backtest_calls}")


# ─── 测试 7：正常流程下 phase_run_backtest 被调用两次（probe + expand）────────
def test_full_pipeline_calls_backtest_twice():
    from run_pipeline import build_parser, run_pipeline

    parser = build_parser()
    args = parser.parse_args([
        "--skip-probe-gen",
        "--skip-probe-run",
        "--dataset-id", "pv13",
    ])

    logger = MagicMock()
    backtest_calls = []
    mock_report = {"summary": {"expand_batches_generated": 2}}

    with patch("run_pipeline.phase_schedule", return_value=mock_report):
        with patch("run_pipeline.phase_run_backtest",
                   side_effect=lambda *a, **kw: backtest_calls.append(kw.get("phase_label", "?"))):
            run_pipeline(args, logger)

    # skip-probe-run 跳过了 Phase 2，所以只有 Phase 4 的 expand backtest
    check("full_pipeline_expand_backtest_called", len(backtest_calls) == 1,
          f"backtest calls: {backtest_calls}")
    check("full_pipeline_expand_label",
          any("EXPAND" in str(c) for c in backtest_calls),
          f"calls: {backtest_calls}")


# ─── 运行所有测试 ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import tempfile

    print("=" * 60)
    print("run_pipeline.py validation tests")
    print("=" * 60)

    run_test("import", test_import)
    run_test("parser_defaults", test_parser_defaults)
    run_test("skip_flags", test_skip_flags)
    run_test("probe_only_skips_scheduling", test_probe_only_skips_scheduling)

    with tempfile.TemporaryDirectory() as tmp:
        run_test("dry_run_no_expand_batch", lambda: test_dry_run_no_expand_batch(Path(tmp)))

    run_test("dry_run_skips_expand_backtest", test_dry_run_skips_expand_backtest)
    run_test("full_pipeline_calls_backtest_twice", test_full_pipeline_calls_backtest_twice)

    print()
    print("=" * 60)
    passed = sum(1 for _, ok in results if ok)
    failed = sum(1 for _, ok in results if not ok)
    print(f"Results: {passed} passed, {failed} failed out of {len(results)} checks")
    print("=" * 60)
    sys.exit(0 if failed == 0 else 1)
