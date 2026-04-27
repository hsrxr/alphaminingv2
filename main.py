import argparse
import json
import re
from copy import deepcopy
from collections.abc import Iterable
from datetime import datetime
from itertools import product
from pathlib import Path

from datafields_store import fetch_and_store_datafields


DEFAULT_BATCH_SIZE = 500
DEFAULT_TEMPLATE_DOC = "template_catalog.json"
DEFAULT_OPERATORS_DOC = "wq_operators_cleaned.json"
DEFAULT_OPERATOR_SLOT_MAP_DOC = "common_operator_slot_mappings.json"
DEFAULT_MAX_PER_TEMPLATE = 5000
DEFAULT_MAX_GENERATED = 20000


SIMULATION_SETTINGS = {
    "instrumentType": "EQUITY",
    "region": "USA",
    "universe": "TOP3000",
    "delay": 1,
    "decay": 6,
    "neutralization": "MARKET",
    "truncation": 0.08,
    "pasteurization": "ON",
    "unitHandling": "VERIFY",
    "nanHandling": "OFF",
    "language": "FASTEXPR",
    "visualization": False,
}


SETTING_KEY_ALIASES = {
    "language": "language",
    "instrumenttype": "instrumentType",
    "region": "region",
    "universe": "universe",
    "delay": "delay",
    "neutralization": "neutralization",
    "decay": "decay",
    "truncation": "truncation",
    "pasteurization": "pasteurization",
    "unithandling": "unitHandling",
    "nanhandling": "nanHandling",
    "visualization": "visualization",
}


def load_template_catalog(template_doc: Path) -> dict:
    with open(template_doc, encoding="utf-8") as file_handle:
        payload = json.load(file_handle)
    if "templates" not in payload or not isinstance(payload["templates"], list):
        raise ValueError("Template catalog must contain a templates list.")
    return payload


def load_operator_names(operators_doc: Path) -> set[str]:
    with open(operators_doc, encoding="utf-8") as file_handle:
        rows = json.load(file_handle)

    names = set()
    for row in rows:
        syntax = str(row.get("operator_syntax", ""))
        matches = re.findall(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(", syntax)
        names.update(matches)
    return names


def load_common_operator_slot_mappings(mapping_doc: Path) -> dict[str, list[str]]:
    with open(mapping_doc, encoding="utf-8") as file_handle:
        payload = json.load(file_handle)
    mappings = payload.get("operator_slot_mappings", {})
    if not isinstance(mappings, dict):
        raise ValueError("operator_slot_mappings must be an object.")

    normalized = {}
    for slot_name, values in mappings.items():
        if isinstance(values, list) and values:
            normalized[str(slot_name)] = [str(value) for value in values]
    return normalized


def validate_template_names(catalog: dict) -> None:
    naming = catalog.get("naming", {})
    rule = naming.get("regex", "")
    if not rule:
        raise ValueError("Template catalog naming.regex is required.")

    compiled = re.compile(rule)
    for template in catalog["templates"]:
        template_id = str(template.get("template_id", ""))
        if not compiled.fullmatch(template_id):
            raise ValueError(f"Template id does not match naming rule: {template_id}")


def parse_selected_template_ids(value: str, all_template_ids: list[str]) -> list[str]:
    if not value or value.upper() == "ALL":
        return all_template_ids
    selected = [item.strip() for item in value.split(",") if item.strip()]
    unknown = sorted(set(selected) - set(all_template_ids))
    if unknown:
        raise ValueError(f"Unknown template ids: {unknown}")
    return selected


def load_slot_overrides(path: str) -> dict:
    if not path:
        return {}
    with open(path, encoding="utf-8") as file_handle:
        payload = json.load(file_handle)
    if not isinstance(payload, dict):
        raise ValueError("Slot overrides must be a JSON object.")
    return payload


def canonical_setting_key(raw_key: str) -> str:
    key = re.sub(r"[^a-z0-9]", "", str(raw_key).lower())
    return SETTING_KEY_ALIASES.get(key, str(raw_key))


def load_settings_grid(path: str) -> dict[str, list]:
    if not path:
        return {}
    with open(path, encoding="utf-8") as file_handle:
        payload = json.load(file_handle)
    if not isinstance(payload, dict):
        raise ValueError("Settings grid file must be a JSON object.")

    normalized_grid: dict[str, list] = {}
    for raw_key, raw_values in payload.items():
        canonical_key = canonical_setting_key(raw_key)
        if canonical_key not in SIMULATION_SETTINGS:
            raise ValueError(f"Unsupported setting key in settings grid: {raw_key}")
        if not isinstance(raw_values, list) or not raw_values:
            raise ValueError(f"Settings grid value must be a non-empty list: {raw_key}")
        normalized_grid[canonical_key] = raw_values
    return normalized_grid


def iter_settings(base_settings: dict, settings_grid: dict[str, list]) -> Iterable[dict]:
    if not settings_grid:
        yield deepcopy(base_settings)
        return

    keys = list(settings_grid.keys())
    value_lists = [settings_grid[key] for key in keys]
    for values in product(*value_lists):
        candidate = deepcopy(base_settings)
        for key, value in zip(keys, values):
            candidate[key] = value
        yield candidate


def ordered_placeholders(expression: str) -> list[str]:
    placeholders = []
    seen = set()
    for match in re.finditer(r"<([A-Za-z0-9_]+)>", expression):
        key = match.group(1)
        if key not in seen:
            placeholders.append(key)
            seen.add(key)
    return placeholders


# ─────────────────────────────────────────────────────────────────────────────
# 改进 2.1：修复 search_domain 字段过滤逻辑
#   - fallback_to_all 默认改为 False，过滤结果为空时抛出异常而非静默回退
#   - 新增 require_match 参数：当为 True 时，过滤结果为空直接 raise，不允许 fallback
# ─────────────────────────────────────────────────────────────────────────────
def apply_dataset_field_domain(datafields: list[str], slot_def: dict, slot_name: str = "") -> list[str]:
    """Apply search_domain filters to narrow down dataset field candidates.

    include_regex semantics: OR logic — a field is kept if it matches ANY of
    the patterns. This allows a slot to accept multiple disjoint field families
    (e.g. ["historical_volatility", "parkinson_volatility"]).

    exclude_regex semantics: AND logic — a field is removed if it matches ANY
    of the exclude patterns.

    Breaking change from original:
    - ``fallback_to_all`` now defaults to ``False`` instead of ``True``.
    - When the filtered list is empty and ``fallback_to_all`` is False, a
      ``ValueError`` is raised immediately so misconfigured regexes are caught
      at generation time rather than silently producing semantically wrong factors.
    """
    domain = slot_def.get("search_domain", {})
    if not isinstance(domain, dict):
        return list(datafields)

    include_regex = domain.get("include_regex", [])
    exclude_regex = domain.get("exclude_regex", [])
    # Default changed to False: fallback must be explicitly opted-in.
    fallback_to_all = bool(domain.get("fallback_to_all", False))
    max_candidates = int(domain.get("max_candidates", 0) or 0)

    # include_regex: OR logic — keep fields matching ANY pattern.
    # (Previous AND logic was a bug: chaining filters would eliminate all fields
    # when patterns matched disjoint subsets, e.g. ["historical_volatility",
    # "parkinson_volatility"] would first keep only historical_* then filter
    # those for parkinson_* yielding an empty list.)
    if include_regex:
        compiled_includes = [re.compile(str(p), re.IGNORECASE) for p in include_regex]
        filtered = [
            field for field in datafields
            if any(c.search(field) for c in compiled_includes)
        ]
    else:
        filtered = list(datafields)

    # exclude_regex: remove fields matching ANY pattern.
    for pattern in exclude_regex:
        compiled = re.compile(str(pattern), re.IGNORECASE)
        filtered = [field for field in filtered if not compiled.search(field)]

    if not filtered:
        if fallback_to_all:
            print(
                f"[WARN] Slot '{slot_name}': search_domain filters matched 0 fields; "
                f"falling back to all {len(datafields)} fields as configured."
            )
            filtered = list(datafields)
        else:
            raise ValueError(
                f"Slot '{slot_name}': search_domain filters matched 0 fields from "
                f"{len(datafields)} candidates. "
                f"include_regex={include_regex}, exclude_regex={exclude_regex}. "
                f"Fix the regex patterns or set fallback_to_all=true to allow fallback."
            )

    if max_candidates > 0:
        filtered = filtered[:max_candidates]
    return filtered


def build_dataset_field_candidates(template: dict, datafields: list[str], field_role_mode: str) -> dict[str, list[str]]:
    slots = template.get("slots", {})
    dataset_slots = [name for name, slot in slots.items() if slot.get("source") == "dataset_field"]
    if not dataset_slots:
        return {}

    mode = field_role_mode
    if field_role_mode == "auto":
        has_domain = any(isinstance(slots[name].get("search_domain"), dict) for name in dataset_slots)
        mode = "distinguish" if has_domain else "shared"

    candidates = {}
    for slot_name in dataset_slots:
        if mode == "shared":
            candidates[slot_name] = list(datafields)
        else:
            # Pass slot_name for better error messages.
            candidates[slot_name] = apply_dataset_field_domain(datafields, slots[slot_name], slot_name=slot_name)
    return candidates


# ─────────────────────────────────────────────────────────────────────────────
# 改进 2.2：resolve_slot_values 支持 probe_mode
#   - probe_mode=True 时，优先使用 slot 定义中的 representative_values
#   - 这使得探测批次只使用少量代表性参数，而非全量笛卡尔积
# ─────────────────────────────────────────────────────────────────────────────
def resolve_slot_values(
    slot_name: str,
    slot_def: dict,
    dataset_field_candidates: dict[str, list[str]],
    overrides: dict,
    common_operator_slot_mappings: dict[str, list[str]],
    probe_mode: bool = False,
) -> list[str]:
    """Resolve the candidate values for a single template slot.

    When ``probe_mode`` is True, numeric/categorical slots that define
    ``representative_values`` will use that smaller set instead of the full
    ``values`` list, reducing the Cartesian explosion during the probe phase.
    """
    if slot_name in overrides:
        values = overrides[slot_name]
        if not isinstance(values, list) or not values:
            raise ValueError(f"Override for slot {slot_name} must be a non-empty list.")
        return [str(value) for value in values]

    source = slot_def.get("source", "values")
    if source == "dataset_field":
        candidates = dataset_field_candidates.get(slot_name, [])
        if not candidates:
            raise ValueError(f"Slot {slot_name} requires dataset fields, but candidate list is empty.")
        return candidates

    # In probe mode, prefer representative_values if defined.
    if probe_mode:
        rep_values = slot_def.get("representative_values", [])
        if isinstance(rep_values, list) and rep_values:
            return [str(v) for v in rep_values]

    values = slot_def.get("values", [])
    if isinstance(values, list) and values:
        return [str(value) for value in values]

    if slot_def.get("slot_kind") == "operator" and slot_name in common_operator_slot_mappings:
        return [str(value) for value in common_operator_slot_mappings[slot_name]]

    raise ValueError(f"Slot {slot_name} must define non-empty values or a valid source.")


def validate_operator_slots(
    template: dict,
    operator_names: set[str],
    overrides: dict,
    common_operator_slot_mappings: dict[str, list[str]],
) -> None:
    slots = template.get("slots", {})
    for slot_name, slot_def in slots.items():
        if slot_def.get("slot_kind") != "operator":
            continue
        values = resolve_slot_values(
            slot_name=slot_name,
            slot_def=slot_def,
            dataset_field_candidates={},
            overrides=overrides,
            common_operator_slot_mappings=common_operator_slot_mappings,
        )
        unknown = [value for value in values if value not in operator_names]
        if unknown:
            raise ValueError(
                f"Template {template['template_id']} slot {slot_name} has unknown operators: {unknown}"
            )


def combo_matches_constraints(combo: dict[str, str], constraints: dict) -> bool:
    for left, right in constraints.get("not_equal", []):
        if combo.get(left) == combo.get(right):
            return False
    return True


# ─────────────────────────────────────────────────────────────────────────────
# 改进 3.1：iter_template_expressions 携带 core_id 元数据
#   - 返回 (expression, core_id) 而非仅 expression
#   - core_id 由模板定义的 core_slots 字段决定（哪些 slot 构成 Core）
#   - 若未定义 core_slots，则以所有 dataset_field 类型的 slot 作为 Core
# ─────────────────────────────────────────────────────────────────────────────
def compute_core_id(template: dict, combo: dict[str, str]) -> str:
    """Derive a stable core identifier from the combination of core-defining slots.

    The ``core_slots`` list in the template definition specifies which slots
    form the semantic core of the expression (e.g. the field pair that drives
    the signal). Wrapping parameters like smoothing windows or group operators
    are excluded so that variants sharing the same core can be grouped together
    during evaluation.
    """
    slots = template.get("slots", {})
    core_slot_names: list[str] = template.get("core_slots", [])

    if not core_slot_names:
        # Default: treat all dataset_field slots as the core.
        core_slot_names = [name for name, slot in slots.items() if slot.get("source") == "dataset_field"]

    parts = [f"{name}={combo[name]}" for name in core_slot_names if name in combo]
    return "|".join(parts)


def iter_template_expressions(
    template: dict,
    dataset_field_candidates: dict[str, list[str]],
    slot_overrides: dict,
    common_operator_slot_mappings: dict[str, list[str]],
    max_per_template: int,
    probe_mode: bool = False,
) -> Iterable[tuple[str, str]]:
    """Yield (expression, core_id) pairs for a single template.

    ``probe_mode=True`` restricts numeric/categorical slots to their
    ``representative_values``, producing a much smaller probe batch.
    """
    expression = template["expression"]
    slots = template.get("slots", {})
    placeholders = ordered_placeholders(expression)
    constraints = template.get("constraints", {})

    value_lists = []
    for slot_name in placeholders:
        if slot_name not in slots:
            raise ValueError(f"Template {template['template_id']} missing slot definition: {slot_name}")
        value_lists.append(
            resolve_slot_values(
                slot_name=slot_name,
                slot_def=slots[slot_name],
                dataset_field_candidates=dataset_field_candidates,
                overrides=slot_overrides,
                common_operator_slot_mappings=common_operator_slot_mappings,
                probe_mode=probe_mode,
            )
        )

    generated = 0
    for values in product(*value_lists):
        combo = dict(zip(placeholders, values))
        if not combo_matches_constraints(combo, constraints):
            continue

        rendered = expression
        for slot_name, slot_value in combo.items():
            rendered = rendered.replace(f"<{slot_name}>", slot_value)

        core_id = compute_core_id(template, combo)
        yield rendered, core_id
        generated += 1
        if max_per_template > 0 and generated >= max_per_template:
            break


def iter_alpha_requests(
    templates: list[dict],
    datafields: list[str],
    slot_overrides: dict,
    common_operator_slot_mappings: dict[str, list[str]],
    settings_list: list[dict],
    field_role_mode: str,
    max_per_template: int,
    max_generated: int,
    probe_mode: bool = False,
) -> Iterable[dict]:
    generated = 0
    global_overrides = slot_overrides.get("global", {}) if isinstance(slot_overrides.get("global", {}), dict) else {}

    for template in templates:
        template_specific = slot_overrides.get(template["template_id"], {})
        if template_specific and not isinstance(template_specific, dict):
            raise ValueError(f"Overrides for {template['template_id']} must be an object.")

        merged_overrides = dict(global_overrides)
        merged_overrides.update(template_specific)
        dataset_field_candidates = build_dataset_field_candidates(
            template=template,
            datafields=datafields,
            field_role_mode=field_role_mode,
        )

        for expression, core_id in iter_template_expressions(
            template=template,
            dataset_field_candidates=dataset_field_candidates,
            slot_overrides=merged_overrides,
            common_operator_slot_mappings=common_operator_slot_mappings,
            max_per_template=max_per_template,
            probe_mode=probe_mode,
        ):
            for settings in settings_list:
                yield {
                    "type": "REGULAR",
                    "settings": settings,
                    "regular": expression,
                    # ── 改进 3.2：在每条因子记录中携带 core_id 和 template_id ──
                    "core_id": core_id,
                    "template_id": template["template_id"],
                }
                generated += 1
                if max_generated > 0 and generated >= max_generated:
                    return


def write_factor_batches(
    alpha_iter: Iterable[dict],
    output_dir: Path,
    dataset_id: str,
    template_id: str,
    batch_size: int,
    generation_context: dict,
) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    run_dir = output_dir / f"{dataset_id}_{template_id}"
    run_dir.mkdir(parents=True, exist_ok=True)

    written_files = []
    batch = []
    batch_index = 1
    timestamp = datetime.now().strftime("%H%M%S")

    for alpha in alpha_iter:
        batch.append(alpha)
        if len(batch) < batch_size:
            continue

        file_path = run_dir / f"{dataset_id}_{template_id}_{timestamp}_batch_{batch_index:04d}.json"
        payload = {
            "dataset_id": dataset_id,
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "count": len(batch),
            "generation_context": generation_context,
            "factors": batch,
        }
        with open(file_path, "w", encoding="utf-8") as file_handle:
            json.dump(payload, file_handle, ensure_ascii=False, indent=2)
        print(f"Wrote factor batch: {file_path} ({len(batch)} factors)")

        written_files.append(file_path)
        batch = []
        batch_index += 1

    if batch:
        file_path = run_dir / f"{dataset_id}_{template_id}_{timestamp}_batch_{batch_index:04d}.json"
        payload = {
            "dataset_id": dataset_id,
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "count": len(batch),
            "generation_context": generation_context,
            "factors": batch,
        }
        with open(file_path, "w", encoding="utf-8") as file_handle:
            json.dump(payload, file_handle, ensure_ascii=False, indent=2)
        print(f"Wrote factor batch: {file_path} ({len(batch)} factors)")
        written_files.append(file_path)

    return written_files


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate factors from template catalog and dataset fields.")
    parser.add_argument("--dataset-id", default="pv13", help="Dataset id used to fetch data fields.")
    parser.add_argument("--data-type", default="GROUP", help="Data field type filter.")
    parser.add_argument("--template-doc", default=DEFAULT_TEMPLATE_DOC, help="Template catalog JSON file.")
    parser.add_argument("--operators-doc", default=DEFAULT_OPERATORS_DOC, help="WorldQuant operators JSON file.")
    parser.add_argument(
        "--operator-slot-map-doc",
        default=DEFAULT_OPERATOR_SLOT_MAP_DOC,
        help="Persistent common mapping from template operator slots to WQ operators.",
    )
    parser.add_argument(
        "--template-ids",
        default="ALL",
        help="Comma-separated template ids. Use ALL to include all templates in catalog.",
    )
    parser.add_argument(
        "--slot-overrides-file",
        default="",
        help="Optional JSON file that overrides slot values per template.",
    )
    parser.add_argument(
        "--field-role-mode",
        choices=["auto", "shared", "distinguish"],
        default="auto",
        help="How multi-field templates choose field pools for different field roles.",
    )
    parser.add_argument(
        "--settings-grid-file",
        default="",
        help="Optional JSON file for searching combinations of simulation settings.",
    )
    parser.add_argument(
        "--datafields-dir",
        default="datafields_cache",
        help="Directory used to persist fetched data-field pages.",
    )
    parser.add_argument("--output-dir", default="factor_batches", help="Directory to store factor batch files.")
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE, help="Number of factors per batch file.")
    parser.add_argument("--max-per-template", type=int, default=DEFAULT_MAX_PER_TEMPLATE, help="Generation cap per template.")
    parser.add_argument("--max-generated", type=int, default=DEFAULT_MAX_GENERATED, help="Global generation cap.")
    parser.add_argument("--instrument-type", default="EQUITY", help="Instrument type for data-field fetch.")
    parser.add_argument("--region", default="USA", help="Region filter for data-field fetch.")
    parser.add_argument("--delay", type=int, default=1, help="Delay filter for data-field fetch.")
    parser.add_argument("--universe", default="TOP3000", help="Universe filter for data-field fetch.")
    parser.add_argument("--search", default="", help="Optional search term for field fetch.")
    parser.add_argument("--test-years", type=int, default=1, help="Metadata only: test period years.")
    parser.add_argument("--test-months", type=int, default=0, help="Metadata only: test period months.")
    # ── 改进 2.3：新增 --probe 模式开关 ──
    parser.add_argument(
        "--probe",
        action="store_true",
        default=False,
        help=(
            "Probe mode: use representative_values instead of full values for each slot. "
            "Generates a small representative batch per core for quick evaluation. "
            "Use adaptive_scheduler.py to decide whether to expand to full search."
        ),
    )
    args = parser.parse_args()

    dataset_fields_df = fetch_and_store_datafields(
        dataset_id=args.dataset_id,
        data_type=args.data_type,
        output_dir=Path(args.datafields_dir),
        instrument_type=args.instrument_type,
        region=args.region,
        delay=args.delay,
        universe=args.universe,
        search=args.search,
    )
    if dataset_fields_df.empty or "id" not in dataset_fields_df.columns:
        raise RuntimeError("No data fields were returned from the API.")
    dataset_field_ids = dataset_fields_df["id"].dropna().astype(str).tolist()

    catalog = load_template_catalog(Path(args.template_doc))
    validate_template_names(catalog)

    operator_names = load_operator_names(Path(args.operators_doc))
    common_operator_slot_mappings = load_common_operator_slot_mappings(Path(args.operator_slot_map_doc))

    all_template_ids = [template["template_id"] for template in catalog["templates"]]
    selected_ids = parse_selected_template_ids(args.template_ids, all_template_ids)
    slot_overrides = load_slot_overrides(args.slot_overrides_file)
    settings_grid = load_settings_grid(args.settings_grid_file)
    settings_list = list(iter_settings(SIMULATION_SETTINGS, settings_grid))

    selected_templates = [template for template in catalog["templates"] if template["template_id"] in selected_ids]
    applicable_templates = []
    for template in selected_templates:
        applicable_datasets = template.get("applicable_datasets", [])
        if isinstance(applicable_datasets, list) and applicable_datasets and args.dataset_id not in applicable_datasets:
            print(f"Skip template {template['template_id']} for dataset {args.dataset_id}")
            continue
        applicable_templates.append(template)

    if not applicable_templates:
        raise RuntimeError("No templates left after dataset applicability filtering.")

    print(f"Selected {len(applicable_templates)} template(s), {len(settings_list)} settings combination(s).")
    if args.probe:
        print("Probe mode enabled: using representative_values for numeric/categorical slots.")

    for template in applicable_templates:
        template_override = slot_overrides.get(template["template_id"], {})
        global_override = slot_overrides.get("global", {})
        merged_override = {}
        if isinstance(global_override, dict):
            merged_override.update(global_override)
        if isinstance(template_override, dict):
            merged_override.update(template_override)
        validate_operator_slots(
            template=template,
            operator_names=operator_names,
            overrides=merged_override,
            common_operator_slot_mappings=common_operator_slot_mappings,
        )

    alpha_iter = iter_alpha_requests(
        templates=applicable_templates,
        datafields=dataset_field_ids,
        slot_overrides=slot_overrides,
        common_operator_slot_mappings=common_operator_slot_mappings,
        settings_list=settings_list,
        field_role_mode=args.field_role_mode,
        max_per_template=max(1, args.max_per_template),
        max_generated=max(1, args.max_generated),
        probe_mode=args.probe,
    )

    # ── 改进 3.3：在 generation_context 中记录 probe_mode ──
    written_files = write_factor_batches(
        alpha_iter=alpha_iter,
        output_dir=Path(args.output_dir),
        dataset_id=args.dataset_id,
        template_id=args.template_ids if args.template_ids else "all",
        batch_size=max(1, args.batch_size),
        generation_context={
            "template_ids": [template["template_id"] for template in applicable_templates],
            "field_role_mode": args.field_role_mode,
            "settings_grid": settings_grid,
            "settings_count": len(settings_list),
            "probe_mode": args.probe,
            "test_period": {"years": args.test_years, "months": args.test_months},
        },
    )
    print(f"Generation complete. Created {len(written_files)} batch file(s).")


if __name__ == "__main__":
    main()
