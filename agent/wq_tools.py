"""
agent/wq_tools.py — WorldQuant Brain tool layer for the direct agent.

Packs WQ platform capabilities as callable tools:
  Research     — search_operators, get_operator_detail
  Data         — list_datasets, list_fields
  Settings     — get_setting_schema, get_setting_detail
  Validation   — validate_expression
  Execution    — submit_factor, poll_results, get_factor_detail
"""

import json
import re
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urljoin

import requests
from requests.auth import HTTPBasicAuth

from agent.expression_validator import (
    ExpressionValidator,
    _load_known_operators,
    WQ_OPERATORS_FILE,
    DATAFIELDS_CACHE_DIR,
)


# ─── Constants ────────────────────────────────────────────────────────────

API_BASE = "https://api.worldquantbrain.com"
_PROJECT_ROOT = Path(__file__).resolve().parent.parent


# ─── Simulation settings schema ──────────────────────────────────────────

SETTINGS_SCHEMA: dict[str, dict] = {
    "instrumentType": {
        "type": "categorical",
        # "values": ["EQUITY", "FUTURE", "OPTION"],
        "values": ["EQUITY"],
        "default": "EQUITY",
        "description": "Type of instrument to trade. EQUITY for stocks, FUTURE for futures, OPTION for options.",
    },
    "region": {
        "type": "categorical",
        # "values": [
        #     "USA", "EUR", "CHN", "JPN", "GBR", "KOR", "CAN",
        #     "AUS", "TWN", "IND", "BRA", "HKG", "RUS", "MEX", "ZAF",
        # ],
        "values": ["USA"],
        "default": "USA",
        "description": "Trading region/market. USA is the largest and most liquid.",
    },
    "universe": {
        "type": "categorical",
        "values": ["TOP3000", "TOP2000", "TOP1000", "TOP500", "TOP200", "TOPSP500"],
        "default": "TOP3000",
        "description": "Size of the stock universe. TOP3000 = largest 3000 by volume.",
    },
    "delay": {
        "type": "integer",
        "values": [0, 1],
        "default": 1,
        "description": "Days between signal and trade. delay=1 means today's signal trades tomorrow. Higher delay reduces look-ahead bias.",
    },
    "decay": {
        "type": "integer",
        "values": "Any positive integer",
        "default": 5,
        "description": "Half-life in days for the weighting scheme. Lower decay = more responsive to recent signals. Higher decay = smoother, lower turnover.",
    },
    "neutralization": {
        "type": "categorical",
        "values": ["MARKET", "INDUSTRY", "SECTOR", "SUBINDUSTRY", "NONE"],
        "default": "MARKET",
        "description": "Neutralization: Adjust alpha weights such that they sum to zero within each group of the selected type. Risk neutralization level. MARKET = subtract market-wide return. INDUSTRY = neutralize within industries. Finer neutralization reduces capacity.",
    },
    "truncation": {
        "type": "float",
        "values": "0.00-1.00, resolution 0.01",
        "default": 0.08,
        "description": "Daily truncation limit as fraction of the weight distribution. 0.08 means weights are capped at 8% per tail. Higher = more concentrated.",
    },
    "pasteurization": {
        "type": "categorical",
        "values": ["ON", "OFF"],
        "default": "ON",
        "description": "Pasteurization: Replaces operator input values with NaN for instruments not in the universe",
    },
    "unitHandling": {
        "type": "categorical",
        # "values": ["VERIFY", "FLOOR", "DO_NOTHING"],
        "values": ["VERIFY"],
        "default": "VERIFY",
        "description": "Unit Handling: Raises a warning when incompatible units are used in an operator",
    },
    "nanHandling": {
        "type": "categorical",
        "values": ["ON", "OFF"],
        "default": "OFF",
        "description": "Allows aggregation operators to output numeric values when input values are NaN for a given instrument and date. When ON, replaces NaN values with 0. When OFF, NaN values propagate and may cause no-trade days.",
    },
    "Test period": {
        "type": "categorical",
        "values": ["0", "1", "2", "3", "4", "5", "6"],
        "default": "0",
        "description": "Length of the backtest period. Despite this setting, all backtests to date have been conducted over a fixed five-year period from 2019 to 2023.",
    },
}


# ─── Result types ────────────────────────────────────────────────────────

@dataclass
class FactorResult:
    """Result of a single factor backtest submission."""
    job_id: str                  # Brain progress URL
    expression: str
    status: str                  # "pending" | "running" | "completed" | "failed"
    alpha_id: str | None = None
    metrics: dict = field(default_factory=dict)
    error: str = ""


@dataclass
class SubmissionResult:
    """Returned after submitting a factor."""
    job_id: str
    expression: str
    status: str                  # "submitted" | "failed"
    error: str = ""


# ─── Tool layer ──────────────────────────────────────────────────────────

class WQTools:
    """Collection of tools the direct agent can call.

    Usage::

        tools = WQTools()
        tools.login()
        tools.list_datasets()
        tools.search_operators("return")
        result = tools.submit_factor("group_rank(returns, industry)", {...}, "pv1")
    """

    def __init__(self, operators_path: str | Path = WQ_OPERATORS_FILE):
        self.operators_path = Path(operators_path)
        self._all_operators: list[dict] = []
        self._validators: dict[str, ExpressionValidator] = {}
        self._session_manager: Any = None  # Lazy BrainSessionManager
        self._username: str = ""
        self._password: str = ""
        self._active_jobs: dict[str, FactorResult] = {}

    # ── authentication ──────────────────────────────────────────────────

    def load_credentials(self) -> tuple[str, str]:
        """Load Brain API credentials from .env or environment."""
        def _parse_dotenv(dotenv_path: Path) -> dict[str, str]:
            values: dict[str, str] = {}
            if not dotenv_path.exists():
                return values
            with open(dotenv_path, encoding="utf-8") as fh:
                for line in fh:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    k, v = line.split("=", 1)
                    values[k.strip()] = v.strip().strip('"').strip("'")
            return values

        dotenv_path = _PROJECT_ROOT / ".env"
        dotenv_values = _parse_dotenv(dotenv_path)

        import os
        username = dotenv_values.get("BRAIN_USERNAME") or os.environ.get("BRAIN_USERNAME")
        password = dotenv_values.get("BRAIN_PASSWORD") or os.environ.get("BRAIN_PASSWORD")

        if not username or not password:
            raise RuntimeError(
                "Brain credentials not found. Set BRAIN_USERNAME/BRAIN_PASSWORD "
                "in .env or environment variables."
            )
        return username, password

    def login(self, username: str = "", password: str = "") -> bool:
        """Authenticate with Brain API. Returns True on success."""
        if username and password:
            self._username = username
            self._password = password
        else:
            self._username, self._password = self.load_credentials()

        session = requests.Session()
        session.auth = HTTPBasicAuth(self._username, self._password)
        resp = session.post(f"{API_BASE}/authentication", timeout=30)
        if resp.status_code != 201:
            raise RuntimeError(f"Brain authentication failed: {resp.status_code} {resp.text}")
        self._session_manager = session
        return True

    def _ensure_session(self):
        if self._session_manager is None:
            self.login()

    # ── Tool 1: list_datasets ──────────────────────────────────────────

    def list_datasets(self) -> list[dict]:
        """List all locally cached datasets with field counts."""
        results: list[dict] = []
        if not DATAFIELDS_CACHE_DIR.exists():
            return results

        for ds_dir in sorted(DATAFIELDS_CACHE_DIR.iterdir()):
            if not ds_dir.is_dir():
                continue
            desc = self._read_dataset_description(ds_dir)
            field_count = self._count_fields(ds_dir)
            results.append({
                "dataset_id": ds_dir.name,
                "field_count": field_count,
                "description": desc,
            })
        return results

    @staticmethod
    def _read_dataset_description(ds_dir: Path) -> str:
        desc_file = ds_dir / "description.md"
        if not desc_file.exists():
            return ""
        with open(desc_file, encoding="utf-8") as fh:
            for line in fh:
                if "Category" in line:
                    return line.strip()
        return ""

    @staticmethod
    def _count_fields(ds_dir: Path) -> int:
        runs = sorted(p for p in ds_dir.iterdir() if p.is_dir())
        if not runs:
            return 0
        latest = runs[-1]
        count = 0
        for page_file in sorted(latest.glob("page_*.json")):
            try:
                data = json.loads(page_file.read_text(encoding="utf-8"))
                count += len(data.get("results", []))
            except (OSError, json.JSONDecodeError):
                continue
        return count

    # ── Tool 2: list_fields ────────────────────────────────────────────

    def list_fields(self, dataset_id: str) -> list[dict]:
        """List all field IDs and descriptions in a dataset (lightweight overview)."""
        cache = self._load_field_cache(dataset_id)
        return [{"id": f["id"], "description": f["description"]} for f in cache]

    # ── Tool 3: get_field_detail ──────────────────────────────────────

    def get_field_detail(self, field_id: str, dataset_id: str) -> dict | None:
        """Get complete metadata for a specific data field."""
        cache = self._load_field_cache(dataset_id)
        for f in cache:
            if f["id"] == field_id:
                return dict(f)
        return None

    def _load_field_cache(self, dataset_id: str) -> list[dict]:
        """Load all raw field entries for a dataset (cached internally)."""
        if not hasattr(self, "_field_cache_store"):
            self._field_cache_store: dict[str, list[dict]] = {}

        if dataset_id in self._field_cache_store:
            return self._field_cache_store[dataset_id]

        ds_dir = DATAFIELDS_CACHE_DIR / dataset_id
        if not ds_dir.exists():
            return []

        runs = sorted(p for p in ds_dir.iterdir() if p.is_dir())
        if not runs:
            return []
        latest = runs[-1]

        fields: list[dict] = []
        for page_file in sorted(latest.glob("page_*.json")):
            try:
                data = json.loads(page_file.read_text(encoding="utf-8"))
                for result in data.get("results", []):
                    fields.append(self._normalize_field(result))
            except (OSError, json.JSONDecodeError):
                continue

        self._field_cache_store[dataset_id] = fields
        return fields

    @staticmethod
    def _normalize_field(raw: dict) -> dict:
        """Normalise a raw Brain field entry into a clean dict."""
        return {
            "id": raw.get("id", ""),
            "description": raw.get("description", ""),
            "dataset": raw.get("dataset", {}).get("id", ""),
            "category": raw.get("category", {}).get("name", ""),
            "subcategory": raw.get("subcategory", {}).get("name", ""),
            "region": raw.get("region", ""),
            "delay": raw.get("delay", ""),
            "universe": raw.get("universe", ""),
            "type": raw.get("type", ""),
            "dateCoverage": raw.get("dateCoverage", 0),
            "coverage": raw.get("coverage", 0),
            "userCount": raw.get("userCount", 0),
            "alphaCount": raw.get("alphaCount", 0),
            "themes": raw.get("themes", []),
        }

    # ── Tool 3: search_operators ───────────────────────────────────────

    def search_operators(self, keyword: str) -> list[dict]:
        """Search operators by keyword in name, summary, or explanation."""
        self._lazy_load_operators()
        keyword_lower = keyword.lower()
        results: list[dict] = []

        for op in self._all_operators:
            syntax = op.get("operator_syntax", "")
            summary = op.get("summary", "")
            detail = op.get("detailed_explanation", "")
            name = syntax.split("(")[0].strip() if "(" in syntax else syntax

            if (keyword_lower in name.lower()
                or keyword_lower in summary.lower()
                or keyword_lower in detail.lower()):
                results.append({
                    "name": name,
                    "syntax": syntax,
                    "summary": summary,
                    "level": op.get("level", ""),
                })
        return results

    # ── Tool 4: get_operator_detail ────────────────────────────────────

    def get_operator_detail(self, name: str) -> dict | None:
        """Get full detail (syntax, summary, explanation, examples) for one operator."""
        self._lazy_load_operators()
        name_lower = name.lower()

        for op in self._all_operators:
            syntax = op.get("operator_syntax", "")
            op_name = syntax.split("(")[0].strip() if "(" in syntax else syntax
            if op_name.lower() == name_lower:
                return {
                    "name": op_name,
                    "syntax": syntax,
                    "summary": op.get("summary", ""),
                    "level": op.get("level", ""),
                    "detailed_explanation": op.get("detailed_explanation", ""),
                }
        return None

    def _lazy_load_operators(self):
        if not self._all_operators:
            path = self.operators_path
            if path.exists():
                self._all_operators = json.loads(path.read_text(encoding="utf-8"))

    # ── Tool 5: list_all_operators ────────────────────────────────────

    def list_all_operators(self) -> list[dict]:
        """Return a compact overview of all 50 operators (name + syntax + summary)."""
        self._lazy_load_operators()
        results: list[dict] = []
        for op in self._all_operators:
            syntax = op.get("operator_syntax", "")
            name = syntax.split("(")[0].strip() if "(" in syntax else syntax
            results.append({
                "name": name,
                "syntax": syntax,
                "summary": op.get("summary", ""),
                "level": op.get("level", ""),
            })
        return results

    # ── Tool 6: get_setting_schema ─────────────────────────────────────

    def get_setting_schema(self) -> dict[str, dict]:
        """Return the full simulation settings schema with defaults and descriptions."""
        return {k: dict(v) for k, v in SETTINGS_SCHEMA.items()}

    # ── Tool 7: get_setting_detail ─────────────────────────────────────

    def get_setting_detail(self, name: str) -> dict | None:
        """Get detail for one simulation setting parameter."""
        name_lower = name.lower().replace("_", "")
        for key, info in SETTINGS_SCHEMA.items():
            if key.lower() == name_lower:
                return {"parameter": key, **info}
        return None

    # ── Tool 8: get_settings_guide ────────────────────────────────────

    _SETTINGS_GUIDE_CACHE: str | None = None
    _SETTINGS_GUIDE_URL = "https://platform.worldquantbrain.com/learn/documentation/create-alphas/simulation-settings"

    def get_settings_guide(self, refresh: bool = False) -> str:
        """Fetch the official Brain simulation settings documentation page.

        Returns the page content as markdown text.  Results are cached in
        memory for the lifetime of the process; pass ``refresh=True`` to
        re-fetch.
        """
        if self._SETTINGS_GUIDE_CACHE is not None and not refresh:
            return self._SETTINGS_GUIDE_CACHE

        try:
            import requests
            resp = requests.get(self._SETTINGS_GUIDE_URL, timeout=30)
            if resp.status_code != 200:
                return f"Failed to fetch settings guide (HTTP {resp.status_code})."
            html = resp.text
            text = self._strip_html(html)
            self._SETTINGS_GUIDE_CACHE = text
            return text
        except requests.RequestException as exc:
            return f"Error fetching settings guide: {exc}"

    @staticmethod
    def _strip_html(html: str) -> str:
        """Crude HTML-to-text: remove tags, decode entities, collapse whitespace."""
        import re
        text = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL)
        text = re.sub(r"<style[^>]*>.*?</style>", "", text, flags=re.DOTALL)
        text = re.sub(r"<[^>]+>", " ", text)
        text = re.sub(r"&nbsp;", " ", text)
        text = re.sub(r"&amp;", "&", text)
        text = re.sub(r"&lt;", "<", text)
        text = re.sub(r"&gt;", ">", text)
        text = re.sub(r"\s+", " ", text).strip()
        return text[:8000]

    # ── Tool 9: validate_expression ────────────────────────────────────

    def validate_expression(self, expression: str, dataset_id: str = "") -> dict:
        """Validate a WQ FASTEXPR expression. Returns {valid, errors, warnings}."""
        if dataset_id not in self._validators:
            self._validators[dataset_id] = ExpressionValidator(self.operators_path)
        validator = self._validators[dataset_id]

        result = validator.validate(expression, dataset_id=dataset_id or None)
        return result.to_dict()

    # ── Tool 10: submit_factor ─────────────────────────────────────────

    def submit_factor(
        self,
        expression: str,
        settings: dict | None = None,
        dataset_id: str = "",
    ) -> SubmissionResult:
        """Submit one factor expression to Brain API for backtest simulation.

        Returns immediately with a ``job_id`` (progress URL). Use ``poll_results``
        to check completion.
        """
        self._ensure_session()

        # Build payload.
        resolved_settings = dict(SETTINGS_SCHEMA)  # copy schema defaults
        payload_settings = {}
        for key, info in SETTINGS_SCHEMA.items():
            payload_settings[key] = info["default"]
        if settings:
            payload_settings.update(settings)
        payload_settings["language"] = "FASTEXPR"

        # Include dataset_id as metadata (not sent to Brain).
        factor_payload = {
            "type": "regular",
            "settings": payload_settings,
            "regular": expression,
        }

        try:
            resp = self._session_manager.post(
                f"{API_BASE}/simulations",
                json=factor_payload,
                timeout=30,
            )
            if resp.status_code not in (200, 201, 202):
                error_body = resp.text[:500]
                return SubmissionResult(
                    job_id="",
                    expression=expression,
                    status="failed",
                    error=f"HTTP {resp.status_code}: {error_body}",
                )

            job_url = resp.headers.get("Location", "")
            if not job_url:
                return SubmissionResult(
                    job_id="",
                    expression=expression,
                    status="failed",
                    error="No Location header in response",
                )

            # Normalise to absolute URL.
            if not job_url.startswith("http"):
                job_url = urljoin(API_BASE, job_url)

            # Store active job.
            self._active_jobs[job_url] = FactorResult(
                job_id=job_url,
                expression=expression,
                status="running",
            )
            return SubmissionResult(
                job_id=job_url,
                expression=expression,
                status="submitted",
            )

        except requests.RequestException as exc:
            return SubmissionResult(
                job_id="",
                expression=expression,
                status="failed",
                error=str(exc),
            )

    # ── Tool 11: poll_results ─────────────────────────────────────────

    def poll_results(self, job_ids: list[str]) -> list[dict]:
        """Poll one or more job URLs for completion.

        Returns a list of dicts, one per job:
          {job_id, status: "running"|"completed"|"failed",
           alpha_id, metrics: {sharpe, turnover, fitness, ...}, error}
        """
        self._ensure_session()
        results: list[dict] = []

        for job_id in job_ids:
            cached = self._active_jobs.get(job_id)
            if cached and cached.status in ("completed", "failed"):
                results.append(self._factor_to_dict(cached))
                continue

            try:
                resp = self._session_manager.get(job_id, timeout=30)
                if resp.status_code == 200:
                    data = resp.json()

                    # Check if simulation is still running (Retry-After header).
                    retry_after = float(resp.headers.get("Retry-After", 0))
                    if retry_after > 0:
                        status = "running"
                        result_obj = self._active_jobs.get(job_id)
                        if result_obj:
                            result_obj.status = "running"
                        results.append({
                            "job_id": job_id,
                            "status": "running",
                            "alpha_id": None,
                            "metrics": {},
                            "error": "",
                        })
                        continue

                    # Simulation finished.
                    alpha_id = data.get("alpha")
                    if not alpha_id:
                        # Finished without alpha (invalid/rejected).
                        result_obj = self._active_jobs.get(job_id)
                        if result_obj:
                            result_obj.status = "failed"
                            result_obj.error = "Simulation finished without alpha id"
                        results.append({
                            "job_id": job_id,
                            "status": "failed",
                            "alpha_id": None,
                            "metrics": {},
                            "error": "Simulation finished without alpha id",
                        })
                        continue

                    # Fetch alpha detail.
                    try:
                        alpha_resp = self._session_manager.get(
                            f"{API_BASE}/alphas/{alpha_id}", timeout=30
                        )
                        alpha_detail = alpha_resp.json() if alpha_resp.status_code == 200 else {}
                    except requests.RequestException:
                        alpha_detail = {}

                    metrics = self._extract_metrics(alpha_detail)

                    if job_id in self._active_jobs:
                        self._active_jobs[job_id].status = "completed"
                        self._active_jobs[job_id].alpha_id = alpha_id
                        self._active_jobs[job_id].metrics = metrics

                    results.append({
                        "job_id": job_id,
                        "status": "completed",
                        "alpha_id": alpha_id,
                        "metrics": metrics,
                        "error": "",
                    })

                elif resp.status_code == 202:
                    # Still processing.
                    result_obj = self._active_jobs.get(job_id)
                    if result_obj:
                        result_obj.status = "running"
                    results.append({
                        "job_id": job_id,
                        "status": "running",
                        "alpha_id": None,
                        "metrics": {},
                        "error": "",
                    })
                else:
                    error_msg = f"HTTP {resp.status_code}: {resp.text[:200]}"
                    result_obj = self._active_jobs.get(job_id)
                    if result_obj:
                        result_obj.status = "failed"
                        result_obj.error = error_msg
                    results.append({
                        "job_id": job_id,
                        "status": "failed",
                        "alpha_id": None,
                        "metrics": {},
                        "error": error_msg,
                    })

            except requests.RequestException as exc:
                result_obj = self._active_jobs.get(job_id)
                if result_obj:
                    result_obj.status = "failed"
                    result_obj.error = str(exc)
                results.append({
                    "job_id": job_id,
                    "status": "failed",
                    "alpha_id": None,
                    "metrics": {},
                    "error": str(exc),
                })

        return results

    # ── Tool 12: get_factor_detail ─────────────────────────────────────

    def get_factor_detail(self, alpha_id: str) -> dict:
        """Fetch detailed backtest metrics for a completed alpha."""
        self._ensure_session()
        try:
            resp = self._session_manager.get(f"{API_BASE}/alphas/{alpha_id}", timeout=30)
            if resp.status_code != 200:
                return {"alpha_id": alpha_id, "error": f"HTTP {resp.status_code}: {resp.text[:200]}"}
            alpha_detail = resp.json()
            metrics = self._extract_metrics(alpha_detail)
            return {
                "alpha_id": alpha_id,
                "metrics": metrics,
                "raw": alpha_detail,
            }
        except requests.RequestException as exc:
            return {"alpha_id": alpha_id, "error": str(exc)}

    # ── job management ─────────────────────────────────────────────────

    def get_active_jobs(self) -> dict[str, FactorResult]:
        """Return all tracked jobs and their current status."""
        return dict(self._active_jobs)

    def completed_jobs(self) -> list[FactorResult]:
        """Return all completed jobs since last call (consumable)."""
        done = [j for j in self._active_jobs.values() if j.status == "completed"]
        # Mark them as consumed so they don't re-trigger.
        for j in done:
            if j.status == "completed":
                j.status = "consumed"
        return done

    def failed_jobs(self) -> list[FactorResult]:
        """Return all failed jobs."""
        return [j for j in self._active_jobs.values() if j.status == "failed"]

    def clear_jobs(self):
        """Clear all tracked job state."""
        self._active_jobs.clear()

    # ── Knowledge base ────────────────────────────────────────────────

    _knowledge_base_path: Path | None = None

    def _ensure_kb(self) -> Path:
        if self._knowledge_base_path is None:
            self._knowledge_base_path = _PROJECT_ROOT / "agent_output" / "knowledge_base.json"
        self._knowledge_base_path.parent.mkdir(parents=True, exist_ok=True)
        if not self._knowledge_base_path.exists():
            self._knowledge_base_path.write_text("[]", encoding="utf-8")
        return self._knowledge_base_path

    def _load_kb(self) -> list[dict]:
        path = self._ensure_kb()
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return []

    def _save_kb(self, entries: list[dict]) -> None:
        path = self._ensure_kb()
        path.write_text(
            json.dumps(entries, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    # ── Tool 13: add_knowledge ────────────────────────────────────────

    def add_knowledge(self, topic: str, insight: str, source: str = "agent") -> dict:
        """Add an experience entry to the persistent knowledge base.

        Parameters
        ----------
        topic : str
            Short category label (e.g. "turnover", "neutralization", "field_choice").
        insight : str
            The insight/experience to record.
        source : str
            "agent" or "user".

        Returns the saved entry dict.
        """
        entries = self._load_kb()
        entry = {
            "id": len(entries) + 1,
            "topic": topic,
            "insight": insight,
            "source": source,
            "timestamp": datetime.now().isoformat(timespec="seconds"),
        }
        entries.append(entry)
        self._save_kb(entries)
        return entry

    # ── Tool 14: search_knowledge ─────────────────────────────────────

    def search_knowledge(self, keyword: str) -> list[dict]:
        """Search the knowledge base by keyword in topic or insight text."""
        kw = keyword.lower()
        entries = self._load_kb()
        return [
            e for e in entries
            if kw in e.get("topic", "").lower()
            or kw in e.get("insight", "").lower()
        ]

    # ── Tool 15: list_knowledge_topics ────────────────────────────────

    def list_knowledge_topics(self) -> list[dict]:
        """List all unique topics in the knowledge base with entry count."""
        entries = self._load_kb()
        topic_counts: dict[str, int] = {}
        for e in entries:
            t = e.get("topic", "uncategorized")
            topic_counts[t] = topic_counts.get(t, 0) + 1
        return [
            {"topic": t, "count": c, "last_updated": max(
                e["timestamp"] for e in entries if e.get("topic") == t
            )}
            for t, c in sorted(topic_counts.items(), key=lambda x: -x[1])
        ]

    # ── internals ──────────────────────────────────────────────────────

    @staticmethod
    def _extract_metrics(alpha_detail: dict) -> dict:
        """Extract key performance metrics from alpha detail."""
        stats = alpha_detail.get("statistics", alpha_detail.get("stats", {}))
        # Brain API may return statistics in various structures.
        if isinstance(stats, dict):
            return {
                "sharpe": stats.get("sharpe", stats.get("mean", 0)),
                "turnover": stats.get("turnover", stats.get("turnoverMean", 0)),
                "fitness": stats.get("fitness", stats.get("fitnessMean", 0)),
                "mean_return": stats.get("meanReturn", stats.get("returnsMean", 0)),
                "std_dev": stats.get("standardDeviation", stats.get("returnsStd", 0)),
                "days": stats.get("numDays", stats.get("days", 0)),
            }
        return {}

    @staticmethod
    def _factor_to_dict(f: FactorResult) -> dict:
        return {
            "job_id": f.job_id,
            "status": f.status,
            "alpha_id": f.alpha_id,
            "expression": f.expression[:120],
            "metrics": f.metrics,
            "error": f.error,
        }


# ─── Quick self-test ─────────────────────────────────────────────────────

def main():
    """CLI entry for testing tools interactively."""
    import argparse
    import sys
    sys.stdout.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="WQ Tools - Interactive test")
    parser.add_argument("--list-datasets", action="store_true")
    parser.add_argument("--list-fields", default="")
    parser.add_argument("--search-ops", default="")
    parser.add_argument("--op-detail", default="")
    parser.add_argument("--setting-detail", default="")
    parser.add_argument("--validate", default="")
    parser.add_argument("--dataset", default="")
    args = parser.parse_args()

    tools = WQTools()

    if args.list_datasets:
        for ds in tools.list_datasets():
            print(f"{ds['dataset_id']:20s}  {ds['field_count']:4d} fields  {ds['description'][:60]}")

    if args.list_fields:
        fields = tools.list_fields(args.list_fields)
        print(f"{args.list_fields}: {len(fields)} fields")
        for f in fields[:10]:
            print(f"  {f['id']:30s}  {f['description'][:60]}")
        if len(fields) > 10:
            print(f"  ... and {len(fields) - 10} more")

    if args.search_ops:
        for op in tools.search_operators(args.search_ops):
            print(f"{op['name']:20s}  {op['syntax'][:40]:40s}  {op['summary'][:60]}")

    if args.op_detail:
        detail = tools.get_operator_detail(args.op_detail)
        if detail:
            print(json.dumps(detail, ensure_ascii=False, indent=2))
        else:
            print(f"Operator '{args.op_detail}' not found.")

    if args.setting_detail:
        detail = tools.get_setting_detail(args.setting_detail)
        if detail:
            print(json.dumps(detail, ensure_ascii=False, indent=2))
        else:
            print(f"Setting '{args.setting_detail}' not found.")

    if args.validate:
        result = tools.validate_expression(args.validate, dataset_id=args.dataset)
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
