"""
agent/llm_logger.py — Complete LLM exchange logger.

Writes full (untruncated) prompt messages and responses to a dedicated
JSON Lines file alongside session.log.  This keeps session.log lean while
preserving complete LLM call details for offline review.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Optional


def log_exchange(
    log_dir: Path,
    *,
    exchange_id: str,
    call_type: str,
    messages: list[dict],
    response: str,
    temperature: float,
    agent: str = "",
    tag: Optional[str] = None,
) -> None:
    """Append one complete LLM exchange to *llm_exchanges.jsonl*.

    Parameters
    ----------
    log_dir : Path
        Session directory (where *session.log* lives).
    exchange_id : str
        Unique identifier for this exchange within the session.
    call_type : str
        Semantic type — ``"analysis"``, ``"discovery"``, ``"explore"``, etc.
    messages : list[dict]
        The **full** messages list sent to the LLM (role + content pairs).
    response : str
        The **full** raw response text from the LLM.
    temperature : float
        Sampling temperature used.
    agent : str
        Short agent name (``"direct"``, ``"explorer"``, etc.).
    tag : str or None
        Optional short annotation (e.g. ``"idea_discovery"``).
    """
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    path = log_dir / "llm_exchanges.jsonl"

    # Serialise messages manually so we don't lose non-serialisable items.
    messages_clean = _clean_messages(messages)

    record = {
        "t": datetime.now().isoformat(timespec="seconds"),
        "exchange_id": exchange_id,
        "call_type": call_type,
        "agent": agent,
        "temperature": temperature,
        "tag": tag,
        "messages_len": len(messages_clean),
        "messages_size_chars": sum(
            len(m["content"]) if isinstance(m["content"], str) else len(str(m["content"]))
            for m in messages_clean
        ),
        "response_size_chars": len(response),
        "messages": messages_clean,
        "response": response,
    }
    if tag:
        record["tag"] = tag

    line = json.dumps(record, ensure_ascii=False, default=str)
    with open(path, "a", encoding="utf-8") as fh:
        fh.write(line + "\n")


def _clean_messages(messages: list[dict]) -> list[dict]:
    """Return a copy of *messages* safe for JSON serialisation."""
    cleaned = []
    for m in messages:
        content = m.get("content", "")
        if not isinstance(content, str):
            content = str(content)
        cleaned.append({"role": m.get("role", "?"), "content": content})
    return cleaned
