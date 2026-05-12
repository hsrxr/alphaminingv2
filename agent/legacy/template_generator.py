"""
template_generator.py — Generate factor templates from financial ideas using LLM.

Converts natural-language financial intuitions into structured template JSON
that can be consumed directly by ``main.py``.

Workflow
────────
  1. User provides a financial idea ("IV minus HV captures volatility risk premium").
  2. LLM generates a candidate template JSON with slots, expression, constraints.
  3. ExpressionValidator verifies operators and field references.
  4. Validated template is written to disk for use in the pipeline.

Usage
─────
  python template_generator.py \\
      --idea "IV minus HV captures volatility risk premium" \\
      --dataset-id option8 \\
      --output-dir generated_templates
"""

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

from agent.llm_client import LLMClient, load_api_key
from agent.expression_validator import (
    ExpressionValidator,
    _load_known_operators,
    _load_cached_fields,
    WQ_OPERATORS_FILE,
)


# ─── Prompt design ──────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are an expert quantitative analyst designing alpha factor templates for the \
WorldQuant Brain platform. Your templates must be precise, economically meaningful, \
and syntactically valid in WQ FASTEXPR.

RULE: Only use operator names from the provided operator list. NEVER invent operators.
RULE: Only reference data fields from the provided field list. NEVER invent fields.
RULE: The expression MUST use `<slot_name>` placeholders — never hardcode a field value.
RULE: Define `core_slots` as the semantically critical slots (usually dataset_field slots).
RULE: For numeric parameters, always provide both `values` and `representative_values`.

Respond with ONLY the raw JSON template — no markdown fences, no explanation.\
"""


def _build_generation_prompt(
    idea: str,
    dataset_id: str,
    available_fields: list[str],
    operator_summary: str,
    existing_templates: list[dict] | None = None,
) -> list[dict]:
    """Build the LLM prompt for template generation."""

    fields_block = "\n".join(f"  - {fid}" for fid in available_fields[:80])
    if len(available_fields) > 80:
        fields_block += f"\n  ... and {len(available_fields) - 80} more fields."

    context = f"""\
## Financial Idea
{idea}

## Target Dataset
{dataset_id}

## Available Data Fields ({len(available_fields)} total)
{fields_block}

## Available Operators
{operator_summary}

## Template JSON Structure (follow this schema exactly)
{{
  "template_id": "TPL_<DOMAIN>_<SIGNAL>_V1",
  "source_name": "short_snake_case_description",
  "description": "One-line description of what this factor measures",
  "expression": "<slot1>(<slot2>(<field_A>, <field_B>), <param>)",
  "core_slots": ["field_A", "field_B"],
  "applicable_datasets": ["{dataset_id}"],
  "slots": {{
    "field_A": {{"source": "dataset_field"}},
    "field_B": {{"source": "dataset_field"}},
    "param": {{
      "values": [3, 5, 10, 21, 63, 126, 252],
      "representative_values": [5, 21, 126]
    }}
  }},
  "constraints": {{
    "not_equal": [["field_A", "field_B"]]
  }}
}}

Guidelines for good templates:
1. Use slot names that describe the economic role (e.g., ``iv_field``, ``hv_field``).
2. Group semantically related field slots as ``core_slots``.
3. Always include ``not_equal`` constraints for field pairs that should not be identical.
4. Choose ``values`` that cover meaningful parameter ranges.
5. Choose ``representative_values`` that span the range (short, medium, long).\
"""

    existing_block = ""
    if existing_templates:
        existing_block = "\nExisting templates for reference (avoid duplication):\n" + \
            "\n".join(
                f"  {t.get('template_id', '?')}: {t.get('description', '')[:80]}"
                for t in existing_templates[:5]
            )

    user = context + existing_block + "\n\nGenerate the template now."

    return [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user},
    ]


# ─── Template generator ─────────────────────────────────────────────────────

class TemplateGenerator:
    """Generate and validate factor templates from financial ideas."""

    def __init__(
        self,
        llm: LLMClient,
        validator: Optional[ExpressionValidator] = None,
        operators_path: str | Path = WQ_OPERATORS_FILE,
    ):
        self.llm = llm
        self.validator = validator or ExpressionValidator(operators_path)
        self.operators_path = Path(operators_path)
        self._last_validation_error: str = ""

        # Cache operator summary.
        operators = _load_known_operators(self.operators_path)
        names = sorted(operators.keys())
        self._operator_summary = (
            f"Available operators ({len(names)} total): "
            + ", ".join(names)
        )

    def idea_to_template(
        self,
        idea: str,
        dataset_id: str,
        output_dir: str = "generated_templates",
        existing_templates: list[dict] | None = None,
        max_retries: int = 2,
        quiet: bool = False,
    ) -> dict | None:
        """Convert a financial idea into a validated template.

        Steps:
          1. Load cached data fields for the dataset.
          2. Ask LLM to generate a template.
          3. Parse and validate the template.
          4. Validate the template's expression.
          5. Write to disk.

        Returns the template dict on success, or ``None`` on failure.
        """
        # 1. Load context.
        available_fields = _load_cached_fields(dataset_id)
        if not available_fields and not quiet:
            print(f"[WARN] No cached fields for {dataset_id}. "
                  "Field references will not be validated against cache.")

        # 2. LLM generation (with retry).
        last_error = ""
        for attempt in range(1, max_retries + 1):
            if not quiet:
                print(f"  Generating template (attempt {attempt}/{max_retries})...",
                      end=" ", flush=True)

            messages = _build_generation_prompt(
                idea=idea,
                dataset_id=dataset_id,
                available_fields=available_fields,
                operator_summary=self._operator_summary,
                existing_templates=existing_templates,
            )

            t0 = time.time()
            try:
                raw = self.llm.chat_structured(messages)
                elapsed = time.time() - t0
            except Exception as exc:
                elapsed = time.time() - t0
                if not quiet:
                    print(f"LLM error ({elapsed:.1f}s): {exc}")
                last_error = str(exc)
                continue

            if not quiet:
                print(f"done ({elapsed:.1f}s)")

            # 3. Validate structure.
            template = self._validate_template_structure(raw, dataset_id)
            if template:
                if not quiet:
                    print(f"  Template: {template['template_id']}")
                # 4. Validate expression.
                expr_result = self.validator.validate(
                    template["expression"], dataset_id=dataset_id
                )
                if expr_result.valid:
                    if not quiet:
                        print(f"  Expression validation: {expr_result.summary}")
                    # 5. Write to disk.
                    self._write_template(template, output_dir, dataset_id)
                    return template
                else:
                    last_error = f"Expression validation: {expr_result.summary}"
                    if not quiet:
                        print(f"  Expression validation failed: {last_error}")
                    for err in expr_result.errors:
                        if not quiet:
                            print(f"    ERROR: {err}")
            else:
                if not quiet:
                    err = self._last_validation_error or "Template structure validation failed"
                    print(f"    Structure validation failed: {err}")
                last_error = self._last_validation_error or "Structure validation failed"

        if not quiet:
            print(f"  [FAILED] All {max_retries} attempts exhausted: {last_error}")
        return None

    # ── template structure validation ─────────────────────────────────────

    def _validate_template_structure(
        self, raw: dict, dataset_id: str
    ) -> dict | None:
        """Check that the LLM response has all required template fields.

        Returns the (possibly modified) template dict on success, or ``None``
        with error details accessible via ``_last_validation_error``.
        """
        self._last_validation_error = ""

        required_top = ("template_id", "expression", "slots")
        for key in required_top:
            if key not in raw:
                self._last_validation_error = f"Missing required field: '{key}'"
                return None

        # Normalise template_id.
        tid = raw.get("template_id", "")
        if not re.match(r"^TPL_[A-Z0-9_]+_V[0-9]+$", tid):
            suggestion = self._suggest_template_id(dataset_id)
            raw["template_id"] = suggestion

        # Ensure expression uses placeholders that have slot definitions.
        placeholders = set(re.findall(r"<([A-Za-z0-9_]+)>", raw.get("expression", "")))
        defined_slots = set(raw.get("slots", {}).keys())
        missing = placeholders - defined_slots
        if missing:
            self._last_validation_error = f"Expression uses undefined slots: {missing}"
            return None

        # Ensure core_slots is defined (default to dataset_field slots).
        if "core_slots" not in raw or not raw["core_slots"]:
            raw["core_slots"] = [
                name for name, slot in raw.get("slots", {}).items()
                if isinstance(slot, dict) and slot.get("source") == "dataset_field"
            ]

        # Add applicable_datasets if missing.
        if "applicable_datasets" not in raw or not raw["applicable_datasets"]:
            raw["applicable_datasets"] = [dataset_id]

        return raw

    @staticmethod
    def _suggest_template_id(dataset_id: str) -> str:
        prefix = dataset_id.upper().replace("-", "_")
        return f"TPL_{prefix}_GEN_V1"

    # ── persistence ───────────────────────────────────────────────────────

    def _write_template(
        self, template: dict, output_dir: str, dataset_id: str
    ) -> Path:
        out_path = Path(output_dir) / dataset_id
        out_path.mkdir(parents=True, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        tid = template["template_id"]
        file_path = out_path / f"{tid}_{timestamp}.json"

        payload = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "source_idea": "",
            "dataset_id": dataset_id,
            "template": template,
        }
        file_path.write_text(
            json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        print(f"\nTemplate saved to {file_path}")
        return file_path


# ─── CLI ────────────────────────────────────────────────────────────────────

def _load_existing_templates(paths: list[str]) -> list[dict]:
    templates: list[dict] = []
    for path in paths:
        try:
            payload = json.loads(Path(path).read_text(encoding="utf-8"))
            tlist = payload.get("templates", [payload] if "template_id" in payload else [])
            templates.extend(tlist)
        except (OSError, json.JSONDecodeError) as exc:
            print(f"[WARN] Cannot load {path}: {exc}")
    return templates


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate factor templates from financial ideas using LLM.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--idea", required=True, help="Financial idea in plain English.")
    parser.add_argument("--dataset-id", required=True, help="Target dataset (e.g. option8, pv13).")
    parser.add_argument(
        "--output-dir", default="generated_templates",
        help="Directory to write generated templates.",
    )
    parser.add_argument(
        "--reference-templates", nargs="*", default=[],
        help="Paths to existing template files for context (avoids duplication).",
    )
    parser.add_argument("--api-key", default="", help="DeepSeek API key.")
    parser.add_argument("--model", default="deepseek-chat", help="LLM model.")
    parser.add_argument("--retries", type=int, default=2, help="Max LLM retries.")
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    api_key = args.api_key or load_api_key()
    llm = LLMClient(api_key=api_key, model=args.model)
    generator = TemplateGenerator(llm=llm)

    existing = _load_existing_templates(args.reference_templates)

    print(f"=== Template Generator ===\n")
    print(f"  Idea:      {args.idea}")
    print(f"  Dataset:   {args.dataset_id}")
    print(f"  Retries:   {args.retries}\n")

    template = generator.idea_to_template(
        idea=args.idea,
        dataset_id=args.dataset_id,
        output_dir=args.output_dir,
        existing_templates=existing or None,
        max_retries=args.retries,
    )

    if template:
        print(f"\nGenerated template: {template['template_id']}")
        print(json.dumps(template, ensure_ascii=False, indent=2))
    else:
        print("\nTemplate generation failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
