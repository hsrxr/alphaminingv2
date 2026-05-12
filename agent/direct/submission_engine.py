"""Submission engine — expression submission, FIFO queue, polling, and retry."""

from datetime import datetime

from agent.config import (
    MAX_CONCURRENT,
    MAX_QUEUE_RETRIES,
    MAX_QUEUE_SIZE,
    MAX_POLL_RETRIES,
)
from agent.utils import _check_named_params, _is_transient_error


class SubmissionEngine:
    """Manages expression submission, the FIFO queue, polling, and retry.

    Responsibilities:
    - Validate and submit expressions to the Brain API
    - FIFO queue for overflow expressions (beyond the 3-job concurrency limit)
    - Poll all active and retry-queue jobs
    - Handle transient errors with retry and permanent failures with reporting
    - Track dedup notes, queue errors, and validation warnings for LLM feedback

    Injected dependencies
    --------------------
    ``job_store`` — to record new jobs and read active/retry state
    ``phase_manager`` — to update direction best-sharpe tracking during polling
    """

    def __init__(self, tools, dataset_id: str, quiet: bool, log_event,
                 job_store, phase_manager):
        self.tools = tools
        self.dataset_id = dataset_id
        self.quiet = quiet
        self._log_event = log_event
        self.job_store = job_store
        self.phase_manager = phase_manager

        self._submission_queue: list[dict] = []
        self._queue_errors: list[dict] = []
        self._validation_warnings: list[str] = []
        self._dedup_notes: list[str] = []

    # ── Per-round state ────────────────────────────────────────────────────

    def clear_per_round_state(self) -> None:
        """Clear state that resets at the beginning of each analysis round.

        Note: ``_validation_warnings`` is NOT cleared here — it is cleared
        after display in ``PromptBuilder.build_common_sections`` so that
        warnings survive across the ``_submit_all → _analyse_and_improve``
        boundary.
        """
        self._dedup_notes.clear()
        self._queue_errors.clear()

    # ── Submit ─────────────────────────────────────────────────────────────

    def submit_all(self, expressions: list[dict]) -> tuple[int, int]:
        """Validate and submit expressions, queuing extras when slots are full.

        Returns ``(submitted_count, rate_limited_count)``.
        """
        # First drain any queued expressions from previous rounds.
        self.drain_queue()

        submitted = 0
        rate_limited = 0

        for ex in expressions:
            expr = ex.get("expression", "").strip()
            if not expr:
                continue

            # Validate before enqueuing.
            validation = self.tools.validate_expression(expr, self.dataset_id)

            # Collect non-fatal warnings.
            expr_warnings = self._check_expression_warnings(expr, validation)
            if expr_warnings:
                self._validation_warnings.extend(expr_warnings)
                if not self.quiet:
                    for w in expr_warnings:
                        print(f"  [WARN] {w}")

            if not validation.get("valid"):
                errors = validation.get("errors", [])
                self._log_event("submit", expression=expr[:100], status="invalid", errors=errors)
                if not self.quiet:
                    print(f"  [INVALID] {expr[:70]}...  {errors}")
                continue

            # Slot available → submit immediately.
            if len(self.job_store.active_jobs) < MAX_CONCURRENT:
                result = self.tools.submit_factor(
                    expr,
                    settings=ex.get("settings", {}),
                    dataset_id=self.dataset_id,
                )

                if result.status == "submitted":
                    submitted += 1
                    self._record_submission(expr, ex, result.job_id)
                    if not self.quiet:
                        print(f"  [SUBMIT] {expr[:70]}...  ->  ...{result.job_id[-16:]}")
                else:
                    is_rate_limit = "429" in (result.error or "")
                    if is_rate_limit:
                        rate_limited += 1
                        self._enqueue(expr, ex)
                        if not self.quiet:
                            print(f"  [RATE_LIMIT→QUEUE] {expr[:70]}...  (queued for retry)")
                    else:
                        self._log_event("submit", expression=expr[:100], status="failed",
                                        error=str(result.error)[:200])
                        if not self.quiet:
                            print(f"  [FAIL] {expr[:70]}...  {result.error[:120]}")
            else:
                # No slot — enqueue for later.
                self._enqueue(expr, ex)
                if not self.quiet:
                    print(f"  [SLOTS FULL→QUEUE] {expr[:70]}...  (queue={len(self._submission_queue)})")

        if submitted == 0 and rate_limited == 0 and not self.quiet:
            print("  [WARN] No expressions submitted (all queued or invalid).")
        return submitted, rate_limited

    def _record_submission(self, expr: str, ex: dict, job_id: str) -> None:
        """Bookkeeping after a successful submission."""
        dedup_note = self.job_store.create_job_record(expr, ex, job_id, self.job_store.iteration)
        if dedup_note:
            self._dedup_notes.append(dedup_note)

    # ── Queue ──────────────────────────────────────────────────────────────

    def _enqueue(self, expr: str, ex: dict) -> None:
        """Push an expression to the FIFO submission queue."""
        entry = {
            "expression": expr,
            "settings": ex.get("settings", {}),
            "rationale": ex.get("rationale", ""),
            "direction_id": ex.get("direction_id", ""),
            "retry_count": 0,
            "enqueued_at": datetime.now().isoformat(),
        }
        self._submission_queue.append(entry)
        self._log_event("queue_enqueue", expression=expr[:100], queue_size=len(self._submission_queue))

        while len(self._submission_queue) > MAX_QUEUE_SIZE:
            dropped = self._submission_queue.pop(0)
            self._log_event("queue_dropped", expression=dropped["expression"][:100], reason="queue_full")

    def drain_queue(self) -> int:
        """Try submitting queued expressions while concurrency slots are free.

        Returns the number of successfully submitted entries.
        """
        submitted = 0
        remaining: list[dict] = []
        idx = 0

        while idx < len(self._submission_queue):
            entry = self._submission_queue[idx]

            if len(self.job_store.active_jobs) >= MAX_CONCURRENT:
                remaining.extend(self._submission_queue[idx:])
                break

            expr = entry.get("expression", "").strip()
            if not expr:
                idx += 1
                continue

            validation = self.tools.validate_expression(expr, self.dataset_id)
            expr_warnings = self._check_expression_warnings(expr, validation)
            if expr_warnings:
                self._validation_warnings.extend(expr_warnings)

            if not validation.get("valid"):
                errors = validation.get("errors", [])
                self._queue_errors.append({
                    "expression": expr[:150],
                    "error": str(errors),
                })
                self._log_event("queue_invalid", expression=expr[:100], errors=errors)
                if not self.quiet:
                    print(f"  [QUEUE INVALID] {expr[:70]}...  {errors}")
                idx += 1
                continue

            result = self.tools.submit_factor(
                expr,
                settings=entry.get("settings", {}),
                dataset_id=self.dataset_id,
            )

            if result.status == "submitted":
                submitted += 1
                self._record_submission(expr, entry, result.job_id)
                self._log_event("queue_dequeue", expression=expr[:100], job_id=result.job_id)
                if not self.quiet:
                    print(f"  [QUEUE→SUBMIT] {expr[:70]}...  ->  ...{result.job_id[-16:]}")
            else:
                is_rate_limit = "429" in (result.error or "")
                if is_rate_limit:
                    entry["retry_count"] += 1
                    if entry["retry_count"] >= MAX_QUEUE_RETRIES:
                        self._log_event("queue_dropped", expression=expr[:100],
                                        retries=entry["retry_count"])
                        if not self.quiet:
                            print(f"  [QUEUE DROPPED] {expr[:70]}...  max retries ({entry['retry_count']})")
                        idx += 1
                        continue
                    remaining.append(entry)
                    if not self.quiet:
                        print(f"  [QUEUE RETRY] {expr[:70]}...  (attempt {entry['retry_count']}/{MAX_QUEUE_RETRIES})")
                else:
                    self._log_event("queue_dropped", expression=expr[:100], error=str(result.error)[:100])
                    if not self.quiet:
                        print(f"  [QUEUE FAIL] {expr[:70]}...  {result.error[:120]}")

            idx += 1

        self._submission_queue = remaining
        return submitted

    def clear_queue_for_directions(self, direction_ids: set[str]) -> int:
        """Remove queue entries belonging to abandoned directions.

        Returns the number of entries removed.
        """
        before = len(self._submission_queue)
        self._submission_queue = [
            e for e in self._submission_queue
            if e.get("direction_id") not in direction_ids
        ]
        removed = before - len(self._submission_queue)
        if removed > 0:
            self._log_event("queue_abandon_cleared", removed=removed)
        return removed

    def queue_size(self) -> int:
        return len(self._submission_queue)

    # ── Polling ────────────────────────────────────────────────────────────

    def poll_all(self) -> None:
        """Poll all active jobs and retry queue, with retry for transient errors."""
        all_ids = (
            list(self.job_store.active_jobs.keys())
            + list(self.job_store._retry_queue.keys())
        )
        if not all_ids:
            return

        try:
            results = self.tools.poll_results(all_ids)
        except Exception as exc:
            self._log_event("poll", jobs=len(all_ids), status="error", error=str(exc)[:200])
            if not self.quiet:
                print(f"  [POLL ERR] {exc}")
            return

        recovered: list[str] = []

        for r in results:
            jid = r["job_id"]
            new_status = r["status"]

            # ── Retry queue jobs ────────────────────────────────────────
            if jid in self.job_store._retry_queue:
                job = self.job_store._retry_queue[jid]
                if new_status == "completed":
                    self.job_store.recover_from_retry(jid, job, r)
                    recovered.append(jid)
                    if not self.quiet:
                        print(f"  [RECOVERED] ...{jid[-16:]}  sharpe={job.get('metrics',{}).get('sharpe','?')}")
                elif new_status == "failed" and not _is_transient_error(r.get("error", "")):
                    self.job_store.mark_retry_failed(jid, job, r)
                    if not self.quiet:
                        print(f"  [FAILED]  ...{jid[-16:]}  {r['error'][:80]}")
                continue

            # ── Active jobs ─────────────────────────────────────────────
            if jid not in self.job_store.active_jobs:
                continue

            job = self.job_store.active_jobs[jid]

            # Transient error → retry up to MAX_POLL_RETRIES
            if new_status == "failed" and r.get("error"):
                retries = job.get("_poll_retries", 0) + 1
                job["_poll_retries"] = retries
                if _is_transient_error(r["error"]):
                    if retries < MAX_POLL_RETRIES:
                        new_status = "running"
                        if not self.quiet:
                            print(f"  [POLL RETRY] ...{jid[-16:]}  ({retries}/{MAX_POLL_RETRIES})")
                    else:
                        self.job_store.move_to_retry(jid, job, r["error"])
                        if not self.quiet:
                            print(f"  [NETWORK ERROR] ...{jid[-16:]}  moved to retry queue")
                        continue
                # Non-transient failure → fall through to set status = "failed"

            job["status"] = new_status

            if new_status == "completed":
                if r.get("metrics"):
                    job["metrics"] = r["metrics"]
                if r.get("alpha_id"):
                    job["alpha_id"] = r["alpha_id"]
                job.pop("_poll_retries", None)
                job.pop("error", None)

                # Update direction best Sharpe tracking.
                if job.get("direction_id"):
                    did = job["direction_id"]
                    sharpe = (job.get("metrics", {}) or {}).get("sharpe", 0) or 0
                    self.phase_manager.update_best_sharpe(did, sharpe, job.get("round", 0))
            elif new_status == "failed":
                if r.get("error"):
                    job["error"] = r["error"]

    # ── Expression warnings ────────────────────────────────────────────────

    @staticmethod
    def _check_expression_warnings(expr: str, validation: dict) -> list[str]:
        """Collect non-fatal warnings from validation and known operator pitfalls."""
        warnings: list[str] = []
        for w in validation.get("warnings", []):
            warnings.append(f"[Validator] {w}")
        pitfall_hints = _check_named_params(expr)
        for hint in pitfall_hints:
            warnings.append(f"[Syntax] {hint}")
        return warnings
