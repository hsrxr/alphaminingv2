"""LLM conversation — message management, chat, tool execution, and logging."""

import json
from typing import Optional

from agent.config import MAX_MSG_CHARS, MAX_TOTAL_CHARS, TIGHT_MSG_CHARS
from agent.llm_client import _extract_json
from agent.llm_logger import log_exchange


class LLMConversation:
    """Owns the ``messages`` list and the LLM client interaction loop.

    Responsibilities:
    - Maintain the ``messages`` list and ``_total_chars`` ceiling
    - Send messages to the LLM, parse JSON responses
    - Dispatch tool calls via ``_execute_tool``
    - Log each exchange to the session directory
    """

    def __init__(self, llm, tools, session_dir, quiet: bool, log_event):
        self.llm = llm
        self.tools = tools
        self.session_dir = session_dir
        self.quiet = quiet
        self._log_event = log_event

        self.messages: list[dict] = []
        self._total_chars = 0
        self._llm_exchange_counter = 0

    # ── Session initialisation ─────────────────────────────────────────────

    def init_messages(self, system: str, user: str) -> None:
        """Set the initial system + user message."""
        self.messages = [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ]
        self._total_chars = len(system) + len(user)

    def set_messages(self, msgs: list[dict]) -> None:
        self.messages = msgs
        self._total_chars = sum(
            len(m.get("content", "")) for m in msgs
            if isinstance(m.get("content"), str)
        )

    # ── Chat ───────────────────────────────────────────────────────────────

    def chat(self, temperature: float = 0.3) -> Optional[dict]:
        """Send messages to the LLM and parse a JSON response."""
        self._llm_exchange_counter += 1
        exchange_id = f"llm_{self._llm_exchange_counter}"
        messages_snapshot = list(self.messages)

        # Log last user message preview.
        last_user = None
        for m in reversed(messages_snapshot):
            if m["role"] == "user":
                raw = m["content"]
                last_user = raw[:500] if isinstance(raw, str) else str(raw)[:500]
                break
        self._log_event(
            "llm_call",
            exchange_id=exchange_id,
            temperature=temperature,
            last_user=last_user,
        )

        try:
            content = self.llm.chat(self.messages, temperature=temperature)
            self.messages.append({"role": "assistant", "content": content})
            self._total_chars += len(content)
            parsed = _extract_json(content)
            rtype = parsed.get("type", "?") if parsed else "parse_failed"

            log_exchange(
                self.session_dir,
                exchange_id=exchange_id,
                call_type="analysis",
                messages=messages_snapshot,
                response=content,
                temperature=temperature,
                agent="direct",
                tag=rtype,
            )

            self._log_event(
                "llm_response", exchange_id=exchange_id, type=rtype, preview=content[:2000]
            )
            return parsed
        except (ValueError, json.JSONDecodeError) as exc:
            self._log_event(
                "llm_response", exchange_id=exchange_id, type="parse_error", error=str(exc)[:200]
            )
            self.append(
                f"Failed to parse your response as JSON: {exc}. "
                "Please respond with valid JSON per the protocol."
            )
            log_exchange(
                self.session_dir,
                exchange_id=exchange_id,
                call_type="analysis",
                messages=messages_snapshot,
                response=f"[ParseError] {exc}",
                temperature=temperature,
                agent="direct",
                tag="parse_error",
            )
            return None
        except RuntimeError as exc:
            if not self.quiet:
                print(f"  [LLM ERR] {exc}")
            self._log_event(
                "llm_response", exchange_id=exchange_id, type="runtime_error", error=str(exc)[:200]
            )
            log_exchange(
                self.session_dir,
                exchange_id=exchange_id,
                call_type="analysis",
                messages=messages_snapshot,
                response=f"[RuntimeError] {exc}",
                temperature=temperature,
                agent="direct",
                tag="runtime_error",
            )
            return None
        except Exception as exc:
            if not self.quiet:
                print(f"  [LLM NET ERR] {exc}")
            self._log_event(
                "llm_response", exchange_id=exchange_id, type="network_error", error=str(exc)[:200]
            )
            log_exchange(
                self.session_dir,
                exchange_id=exchange_id,
                call_type="analysis",
                messages=messages_snapshot,
                response=f"[NetworkError] {exc}",
                temperature=temperature,
                agent="direct",
                tag="network_error",
            )
            return None

    # ── Append ─────────────────────────────────────────────────────────────

    def append(self, content: str | dict) -> None:
        """Append a user message to the conversation."""
        if isinstance(content, dict):
            content = json.dumps(content, ensure_ascii=False, default=str)

        limit = TIGHT_MSG_CHARS if self._total_chars > MAX_TOTAL_CHARS else MAX_MSG_CHARS
        if len(content) > limit:
            head = limit * 2 // 3
            tail = limit - head - 20
            content = content[:head] + "\n...(truncated)...\n" + content[-tail:]

        self.messages.append({"role": "user", "content": content})
        self._total_chars += len(content)

    # ── Tool execution ─────────────────────────────────────────────────────

    def execute_tool(self, response: dict) -> dict:
        """Execute a tool call and return the result dict."""
        tool = response.get("tool", "")
        args = response.get("args", {})

        dispatch = {
            "list_datasets": lambda: self.tools.list_datasets(),
            "list_fields": lambda: self.tools.list_fields(
                self._arg(args, "dataset_id", str)
            ),
            "get_field_detail": lambda: self.tools.get_field_detail(
                self._arg(args, "field_id", str),
                self._arg(args, "dataset_id", str),
            ),
            "get_dataset_detail": lambda: self.tools.get_dataset_detail(
                self._arg(args, "dataset_id", str)
            ),
            "list_all_operators": lambda: self.tools.list_all_operators(),
            "search_operators": lambda: self.tools.search_operators(
                self._arg(args, "keyword", str)
            ),
            "get_operator_detail": lambda: self.tools.get_operator_detail(
                self._arg(args, "name", str)
            ),
            "get_setting_schema": lambda: self.tools.get_setting_schema(),
            "get_setting_detail": lambda: self.tools.get_setting_detail(
                self._arg(args, "name", str)
            ),
            "get_settings_guide": lambda: self.tools.get_settings_guide(
                refresh=self._arg(args, "refresh", bool, default=False)
            ),
            "validate_expression": lambda: self.tools.validate_expression(
                self._arg(args, "expression", str),
                dataset_id=self._arg(args, "dataset_id", str, default=""),
            ),
            "search_knowledge": lambda: self.tools.search_knowledge(
                keyword=self._arg(args, "keyword", str, default=""),
                tags=args.get("tags"),
            ),
            "list_knowledge_topics": lambda: self.tools.list_knowledge_topics(),
            "list_knowledge_tags": lambda: self.tools.list_knowledge_tags(),
            "add_knowledge": lambda: self.tools.add_knowledge(
                topic=self._arg(args, "topic", str),
                insight=self._arg(args, "insight", str),
                source=self._arg(args, "source", str, default="agent"),
                tags=args.get("tags"),
            ),
            "web_search": lambda: self.tools.web_search(
                query=self._arg(args, "query", str),
                max_results=self._arg(args, "max_results", int, default=10),
            ),
            "fetch_webpage": lambda: self.tools.fetch_webpage(
                url=self._arg(args, "url", str),
                max_chars=self._arg(args, "max_chars", int, default=8000),
            ),
        }

        handler = dispatch.get(tool)
        if handler is None:
            err = f"Unknown tool. Available: {', '.join(sorted(dispatch))}"
            self._log_event("tool_call", tool=tool, args=args, status="unknown_tool")
            return {"tool": tool, "error": err}

        try:
            payload = handler()
            self._log_event("tool_call", tool=tool, args=args, status="ok")
            return {"tool": tool, "args": args, "result": payload}
        except Exception as exc:
            self._log_event(
                "tool_call", tool=tool, args=args, status="error", error=str(exc)[:200]
            )
            return {"tool": tool, "args": args, "error": str(exc)}

    # ── Arg helper ─────────────────────────────────────────────────────────

    @staticmethod
    def _arg(args: dict, name: str, typ, default=None):
        """Extract *name* from *args* with type coercion."""
        val = args.get(name, default)
        if val is None:
            raise ValueError(f"Missing required argument '{name}'")
        if isinstance(val, typ):
            return val
        try:
            return typ(val)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"Argument '{name}' should be {typ.__name__}, got {type(val).__name__}"
            ) from exc
