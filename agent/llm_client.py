"""
agent_llm_client.py — Unified LLM API client for factor analysis agent.

Supports DeepSeek API (OpenAI-compatible format).
Credentials loaded from DEEPSEEK_API_KEY env var or .env file.
"""

import json
import os
import re
import time
from pathlib import Path
from typing import Optional

import requests


DEFAULT_MODEL = "deepseek-chat"
DEFAULT_BASE_URL = "https://api.deepseek.com"
DEFAULT_MAX_RETRIES = 5
DEFAULT_TIMEOUT = 120
DEFAULT_TEMPERATURE = 0.3
DEFAULT_MAX_TOKENS = 4096


def load_api_key(env_var: str = "DEEPSEEK_API_KEY") -> str:
    """Load API key from .env or environment variable.

    Tries the .env file first (looking for the specified key), then
    falls back to the process environment.  Raises ``RuntimeError``
    when neither source provides a value.
    """
    _AGENT_DIR = Path(__file__).resolve().parent
    _PROJECT_ROOT = _AGENT_DIR.parent
    dotenv_path = _PROJECT_ROOT / ".env"
    if dotenv_path.exists():
        with open(dotenv_path, encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if line.startswith(env_var) and "=" in line:
                    return line.split("=", 1)[1].strip().strip('"').strip("'")

    api_key = os.environ.get(env_var)
    if api_key:
        return api_key

    raise RuntimeError(
        f"{env_var} not found. Set it in .env or as an environment variable."
    )


def _extract_json(text: str) -> dict:
    """Robustly extract and parse the first JSON object from *text*.

    Handles plain JSON, markdown-fenced JSON blocks (`````json … `````),
    and text with leading / trailing noise.
    """
    text = text.strip()

    # Strip markdown code fences.
    if text.startswith("```"):
        lines = text.splitlines()
        # Remove opening fence (```json, ```, etc.)
        start = 1
        end = len(lines)
        if lines[-1].strip() == "```":
            end -= 1
        text = "\n".join(lines[start:end]).strip()

    # Direct parse.
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        pass

    # Fallback: locate the outermost { … } block.
    match = re.search(r"\{[\s\S]*\}", text)
    if match:
        try:
            return json.loads(match.group())
        except json.JSONDecodeError:
            pass

    raise ValueError(f"Could not parse JSON from LLM response:\n{text[:600]}")


class LLMClient:
    """Lightweight client for OpenAI-compatible chat APIs (e.g. DeepSeek)."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = DEFAULT_MODEL,
        base_url: str = DEFAULT_BASE_URL,
        max_retries: int = DEFAULT_MAX_RETRIES,
        timeout: int = DEFAULT_TIMEOUT,
    ):
        self.api_key = api_key or load_api_key()
        self.model = model
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.timeout = timeout

    # ── public API ────────────────────────────────────────────────────────

    def chat(
        self,
        messages: list[dict],
        temperature: float = DEFAULT_TEMPERATURE,
        max_tokens: int = DEFAULT_MAX_TOKENS,
    ) -> str:
        """Send a chat completion request and return the response text."""
        payload = self._build_payload(messages, temperature, max_tokens)
        data = self._request(payload)
        return data["choices"][0]["message"]["content"]

    def chat_structured(
        self,
        messages: list[dict],
        temperature: float = DEFAULT_TEMPERATURE,
    ) -> dict:
        """Send a chat request and parse the response as a JSON object."""
        content = self.chat(messages, temperature=temperature)
        return _extract_json(content)

    # ── internals ─────────────────────────────────────────────────────────

    def _build_payload(self, messages, temperature, max_tokens) -> dict:
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        return {
            "headers": headers,
            "model": self.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

    def _request(self, payload: dict) -> dict:
        headers = payload.pop("headers")
        body = {k: v for k, v in payload.items() if k != "headers"}

        last_error = ""
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = requests.post(
                    f"{self.base_url}/chat/completions",
                    headers=headers,
                    json=body,
                    timeout=self.timeout,
                )
                resp.raise_for_status()
                return resp.json()

            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else "?"
                last_error = f"HTTP {status}: {exc}"
                if status == 429:
                    self._backoff(attempt)
                    continue
                if 500 <= status < 600 and attempt < self.max_retries:
                    self._backoff(attempt)
                    continue
                raise

            except (requests.ConnectionError, requests.Timeout) as exc:
                last_error = f"Network error: {exc}"
                if attempt < self.max_retries:
                    self._backoff(attempt)
                    continue
                raise

            except (KeyError, json.JSONDecodeError) as exc:
                raise RuntimeError(f"Unexpected API response: {exc}")

        raise RuntimeError(
            f"LLM request failed after {self.max_retries} attempts: {last_error}"
        )

    @staticmethod
    def _backoff(attempt: int) -> None:
        time.sleep(min(2 ** attempt, 30))
