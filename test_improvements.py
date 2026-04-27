"""
端到端验证脚本：测试所有改进点是否正确工作。
不依赖 Brain API, 使用 mock 数据进行本地验证。
"""
import json
import sys
import traceback
from pathlib import Path

# 将项目目录加入 sys.path
sys.path.insert(0, str(Path(__file__).parent))

# ─── 导入被测模块 ─────────────────────────────────────────────────────────────
from main import (
    apply_dataset_field_domain,
    build_dataset_field_candidates,
    compute_pipeline_core_id,
    iter_template_expressions,
    resolve_slot_values,
    load_template_catalog,
)
from result_filter import collect_result_rows, build_core_summary
from adaptive_scheduler import aggregate_by_core, classify_core

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


# ═══════════════════════════════════════════════════════════════════════════════
# 测试 1：fallback_to_all=False 时，空过滤结果应抛出 ValueError
# ═══════════════════════════════════════════════════════════════════════════════
def test_fallback_false_raises():
    datafields = ["historical_volatility_30d", "historical_volatility_60d", "parkinson_vol_20d"]
    slot_def = {
        "source": "dataset_field",
        "search_domain": {
            "include_regex": ["implied_volatility"],
            "fallback_to_all": False,
        }
    }
    try:
        apply_dataset_field_domain(datafields, slot_def, slot_name="iv_mean_field")
        check("fallback_false_raises", False, "Expected ValueError but none was raised")
    except ValueError as exc:
        check("fallback_false_raises", True, str(exc)[:80])


# ═══════════════════════════════════════════════════════════════════════════════
# 测试 2：fallback_to_all=True 时，空过滤结果应回退到全量并打印警告
# ═══════════════════════════════════════════════════════════════════════════════
def test_fallback_true_returns_all():
    datafields = ["historical_volatility_30d", "parkinson_vol_20d"]
    slot_def = {
        "source": "dataset_field",
        "search_domain": {
            "include_regex": ["implied_volatility"],
            "fallback_to_all": True,
        }
    }
    result = apply_dataset_field_domain(datafields, slot_def, slot_name="iv_mean_field")
    check("fallback_true_returns_all", result == datafields, f"Got {result}")


# ═══════════════════════════════════════════════════════════════════════════════
# 测试 3：正确的 include_regex 应只返回匹配的字段
# ═══════════════════════════════════════════════════════════════════════════════
def test_include_regex_correct():
    datafields = [
        "implied_volatility_30d", "implied_volatility_60d",
        "historical_volatility_30d", "parkinson_vol_20d",
    ]
    slot_def = {
        "source": "dataset_field",
        "search_domain": {
            "include_regex": ["implied_volatility"],
            "fallback_to_all": False,
        }
    }
    result = apply_dataset_field_domain(datafields, slot_def, slot_name="iv_mean_field")
    expected = ["implied_volatility_30d", "implied_volatility_60d"]
    check("include_regex_correct", result == expected, f"Got {result}")


# ═══════════════════════════════════════════════════════════════════════════════
# 测试 4：probe_mode=True 时，应使用 representative_values
# ═══════════════════════════════════════════════════════════════════════════════
def test_probe_mode_uses_representative_values():
    slot_def = {
        "values": [3, 5, 10, 21, 63, 126, 252],
        "representative_values": [5, 21, 126],
    }
    full_values = resolve_slot_values(
        "smooth_days", slot_def, {}, {}, {}, probe_mode=False
    )
    probe_values = resolve_slot_values(
        "smooth_days", slot_def, {}, {}, {}, probe_mode=True
    )
    check("probe_mode_full_values", full_values == ["3", "5", "10", "21", "63", "126", "252"],
          f"Got {full_values}")
    check("probe_mode_representative_values", probe_values == ["5", "21", "126"],
          f"Got {probe_values}")


# ═══════════════════════════════════════════════════════════════════════════════
# 测试 5：compute_pipeline_core_id 应正确提取 core_slots 定义的字段
# ═══════════════════════════════════════════════════════════════════════════════
def test_compute_pipeline_core_id():
    template = {
        "template_id": "TPL_GROUP_IVHV_SMOOTH_V1",
        "core_slots": ["iv_mean_field", "hv_field"],
        "slots": {},
    }
    combo = {
        "iv_mean_field": "implied_volatility_30d",
        "hv_field": "historical_volatility_30d",
        "smooth_days": "21",
        "group": "industry",
    }
    core_id = compute_pipeline_core_id(template, combo)
    expected = "iv_mean_field=implied_volatility_30d|hv_field=historical_volatility_30d"
    check("compute_pipeline_core_id", core_id == expected, f"Got '{core_id}'")


# ═══════════════════════════════════════════════════════════════════════════════
# 测试 6：iter_template_expressions 在 probe 模式下生成更少的因子
# ═══════════════════════════════════════════════════════════════════════════════
def test_probe_mode_reduces_count():
    template = {
        "template_id": "TPL_GROUP_TS_BASIC_V1",
        "expression": "<group_op>(<ts_op>(<datafield>, <day>), <group>)",
        "core_slots": ["datafield"],
        "slots": {
            "group_op": {"slot_kind": "operator", "values": ["group_rank", "group_mean"]},
            "ts_op": {"slot_kind": "operator", "values": ["ts_mean", "ts_rank"]},
            "datafield": {"source": "dataset_field"},
            "day": {
                "values": [3, 5, 10, 21, 63, 126, 252],
                "representative_values": [5, 21, 126],
            },
            "group": {
                "values": ["market", "sector", "industry"],
                "representative_values": ["industry"],
            },
        },
        "constraints": {},
    }
    datafields = ["field_a", "field_b"]
    candidates = {"datafield": datafields}

    full_exprs = list(iter_template_expressions(template, candidates, {}, {}, 0, probe_mode=False))
    probe_exprs = list(iter_template_expressions(template, candidates, {}, {}, 0, probe_mode=True))

    # full: 2 group_op × 2 ts_op × 2 fields × 7 days × 3 groups = 168
    # probe: 2 group_op × 2 ts_op × 2 fields × 3 days × 1 group = 24
    check("probe_mode_reduces_count_full", len(full_exprs) == 168, f"Full count: {len(full_exprs)}")
    check("probe_mode_reduces_count_probe", len(probe_exprs) == 24, f"Probe count: {len(probe_exprs)}")


# ═══════════════════════════════════════════════════════════════════════════════
# 测试 7：iter_template_expressions 返回的每个元素都包含 pipeline_core_id
# ═══════════════════════════════════════════════════════════════════════════════
def test_iter_returns_pipeline_core_id():
    template = {
        "template_id": "TPL_GROUP_TS_BASIC_V1",
        "expression": "<group_op>(<ts_op>(<datafield>, <day>), <group>)",
        "core_slots": ["datafield"],
        "slots": {
            "group_op": {"slot_kind": "operator", "values": ["group_rank"]},
            "ts_op": {"slot_kind": "operator", "values": ["ts_mean"]},
            "datafield": {"source": "dataset_field"},
            "day": {"values": [21], "representative_values": [21]},
            "group": {"values": ["industry"], "representative_values": ["industry"]},
        },
        "constraints": {},
    }
    candidates = {"datafield": ["field_x"]}
    exprs = list(iter_template_expressions(template, candidates, {}, {}, 0, probe_mode=False))
    check("iter_returns_core_id_count", len(exprs) == 1, f"Count: {len(exprs)}")
    expr, core_id = exprs[0]
    check("iter_returns_pipeline_core_id_value", core_id == "datafield=field_x", f"core_id='{core_id}'")
    check("iter_returns_expression", "field_x" in expr, f"expr='{expr}'")


# ═══════════════════════════════════════════════════════════════════════════════
# 测试 8：template_catalog.json 中的 IVHV 模板配置正确
# ═══════════════════════════════════════════════════════════════════════════════
def test_template_catalog_ivhv():
    catalog = load_template_catalog(Path("template_catalog.json"))
    templates = {t["template_id"]: t for t in catalog["templates"]}
    ivhv = templates.get("TPL_GROUP_IVHV_SMOOTH_V1")
    check("ivhv_template_exists", ivhv is not None)
    if ivhv is None:
        return

    iv_slot = ivhv["slots"]["iv_mean_field"]
    hv_slot = ivhv["slots"]["hv_field"]

    iv_fallback = iv_slot.get("search_domain", {}).get("fallback_to_all", True)
    hv_fallback = hv_slot.get("search_domain", {}).get("fallback_to_all", True)
    check("ivhv_iv_fallback_false", iv_fallback is False, f"fallback_to_all={iv_fallback}")
    check("ivhv_hv_fallback_false", hv_fallback is False, f"fallback_to_all={hv_fallback}")

    iv_regex = iv_slot.get("search_domain", {}).get("include_regex", [])
    hv_regex = hv_slot.get("search_domain", {}).get("include_regex", [])
    check("ivhv_iv_regex", "implied_volatility" in iv_regex, f"iv_regex={iv_regex}")
    check("ivhv_hv_regex_has_hv", any("historical_volatility" in r for r in hv_regex), f"hv_regex={hv_regex}")

    smooth_days = ivhv["slots"]["smooth_days"]
    check("ivhv_representative_values_defined",
          "representative_values" in smooth_days,
          f"smooth_days keys: {list(smooth_days.keys())}")
    check("ivhv_core_slots_defined",
          "core_slots" in ivhv and "iv_mean_field" in ivhv["core_slots"],
          f"core_slots={ivhv.get('core_slots')}")


# ═══════════════════════════════════════════════════════════════════════════════
# 测试 9：build_core_summary 正确聚合 Core 级别指标
# ═══════════════════════════════════════════════════════════════════════════════
def test_build_core_summary():
    mock_rows = [
        {"pipeline_core_id": "iv=iv_30d|hv=hv_30d", "pipeline_template_id": "TPL_A", "status": "ok",
         "sharpe": 1.2, "fitness": 1.1, "returns": 0.15, "turnover": 0.4, "regular": "expr1", "alpha_id": "a1"},
        {"pipeline_core_id": "iv=iv_30d|hv=hv_30d", "pipeline_template_id": "TPL_A", "status": "ok",
         "sharpe": 0.9, "fitness": 0.8, "returns": 0.10, "turnover": 0.5, "regular": "expr2", "alpha_id": "a2"},
        {"pipeline_core_id": "iv=iv_60d|hv=hv_60d", "pipeline_template_id": "TPL_A", "status": "ok",
         "sharpe": 0.3, "fitness": 0.2, "returns": 0.05, "turnover": 0.6, "regular": "expr3", "alpha_id": "a3"},
        {"pipeline_core_id": "iv=iv_30d|hv=hv_30d", "pipeline_template_id": "TPL_A", "status": "failed",
         "sharpe": None, "fitness": None, "returns": None, "turnover": None, "regular": "expr4", "alpha_id": None},
    ]
    summaries = {s["pipeline_core_id"]: s for s in build_core_summary(mock_rows)}

    core1 = summaries.get("iv=iv_30d|hv=hv_30d")
    check("core_summary_core1_exists", core1 is not None)
    if core1:
        check("core_summary_core1_ok_count", core1["ok_count"] == 2, f"ok_count={core1['ok_count']}")
        check("core_summary_core1_total_count", core1["total_count"] == 3, f"total_count={core1['total_count']}")
        check("core_summary_core1_sharpe_mean",
              abs(core1["sharpe_mean"] - 1.05) < 0.01,
              f"sharpe_mean={core1['sharpe_mean']}")
        check("core_summary_core1_best_alpha", core1["best_alpha_id"] == "a1",
              f"best_alpha_id={core1['best_alpha_id']}")

    core2 = summaries.get("iv=iv_60d|hv=hv_60d")
    check("core_summary_core2_exists", core2 is not None)
    if core2:
        check("core_summary_core2_sharpe_mean",
              abs(core2["sharpe_mean"] - 0.3) < 0.01,
              f"sharpe_mean={core2['sharpe_mean']}")


# ═══════════════════════════════════════════════════════════════════════════════
# 测试 10：classify_core 决策逻辑
# ═══════════════════════════════════════════════════════════════════════════════
def test_classify_core():
    def make_stats(sharpe_mean, fitness_mean=None, turnover_mean=None, probe_count=3):
        return {
            "sharpe_mean": sharpe_mean,
            "fitness_mean": fitness_mean,
            "turnover_mean": turnover_mean,
            "probe_count": probe_count,
        }

    check("classify_expand", classify_core(make_stats(1.2, 1.1, 0.4), 1.0, 1.0, 0.7, 0.5, 1) == "EXPAND")
    check("classify_watch_low_sharpe", classify_core(make_stats(0.7), 1.0, 1.0, 0.7, 0.5, 1) == "WATCH")
    check("classify_abandon", classify_core(make_stats(0.2), 1.0, 1.0, 0.7, 0.5, 1) == "ABANDON")
    check("classify_watch_high_turnover", classify_core(make_stats(1.2, 1.1, 0.8), 1.0, 1.0, 0.7, 0.5, 1) == "WATCH")
    check("classify_watch_insufficient_probes",
          classify_core(make_stats(1.5, probe_count=0), 1.0, 1.0, 0.7, 0.5, 1) == "WATCH")


# ═══════════════════════════════════════════════════════════════════════════════
# 测试 11：adaptive_scheduler 的 aggregate_by_core
# ═══════════════════════════════════════════════════════════════════════════════
def test_aggregate_by_core():
    mock_results = [
        {
            "pipeline_core_id": "iv=iv_30d|hv=hv_30d",
            "pipeline_template_id": "TPL_A",
            "status": "ok",
            "regular": "expr1",
            "alpha_id": "a1",
            "alpha_detail": {"sharpe": 1.2, "fitness": 1.1, "returns": 0.15, "turnover": 0.4},
        },
        {
            "pipeline_core_id": "iv=iv_30d|hv=hv_30d",
            "pipeline_template_id": "TPL_A",
            "status": "ok",
            "regular": "expr2",
            "alpha_id": "a2",
            "alpha_detail": {"sharpe": 0.8, "fitness": 0.7, "returns": 0.10, "turnover": 0.5},
        },
        {
            "pipeline_core_id": "iv=iv_60d|hv=hv_60d",
            "pipeline_template_id": "TPL_A",
            "status": "ok",
            "regular": "expr3",
            "alpha_id": "a3",
            "alpha_detail": {"sharpe": 0.2, "fitness": 0.1, "returns": 0.02, "turnover": 0.8},
        },
    ]
    aggregated = aggregate_by_core(mock_results)
    core1 = aggregated.get("iv=iv_30d|hv=hv_30d")
    check("aggregate_core1_exists", core1 is not None)
    if core1:
        check("aggregate_core1_probe_count", core1["probe_count"] == 2, f"probe_count={core1['probe_count']}")
        check("aggregate_core1_sharpe_mean",
              abs(core1["sharpe_mean"] - 1.0) < 0.01,
              f"sharpe_mean={core1['sharpe_mean']}")


# ═══════════════════════════════════════════════════════════════════════════════
# 运行所有测试
# ═══════════════════════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 60)
    print("Running improvement validation tests")
    print("=" * 60)

    run_test("fallback_false_raises", test_fallback_false_raises)
    run_test("fallback_true_returns_all", test_fallback_true_returns_all)
    run_test("include_regex_correct", test_include_regex_correct)
    run_test("probe_mode_uses_representative_values", test_probe_mode_uses_representative_values)
    run_test("compute_pipeline_core_id", test_compute_pipeline_core_id)
    run_test("probe_mode_reduces_count", test_probe_mode_reduces_count)
    run_test("iter_returns_pipeline_core_id", test_iter_returns_pipeline_core_id)
    run_test("template_catalog_ivhv", test_template_catalog_ivhv)
    run_test("build_core_summary", test_build_core_summary)
    run_test("classify_core", test_classify_core)
    run_test("aggregate_by_core", test_aggregate_by_core)

    print()
    print("=" * 60)
    passed = sum(1 for _, ok in results if ok)
    failed = sum(1 for _, ok in results if not ok)
    print(f"Results: {passed} passed, {failed} failed out of {len(results)} checks")
    print("=" * 60)
    sys.exit(0 if failed == 0 else 1)
