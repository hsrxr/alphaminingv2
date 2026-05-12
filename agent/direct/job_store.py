"""Job store — manages active jobs, completed results, retry queue, and expression history."""

from datetime import datetime

from agent.config import MIN_SHARPE, MIN_FITNESS, MIN_TURNOVER, MAX_TURNOVER


class JobStore:
    """Central job state for DirectAgent.

    Owns ``active_jobs``, ``completed_results``, ``_retry_queue``,
    ``_expression_history``, ``_iteration_history``, ``iteration``, and ``converged``.

    All mutation of these collections goes through this class so that
    invariants are maintained in one place.
    """

    def __init__(self, log_event):
        self.active_jobs: dict[str, dict] = {}
        self.completed_results: list[dict] = []
        self.iteration = 0
        self.converged = False
        self._retry_queue: dict[str, dict] = {}
        self._expression_history: list[str] = []
        self._iteration_history: list[dict] = []
        self._log_event = log_event

    # ── Queries ────────────────────────────────────────────────────────────

    def get_finished_jobs(self) -> list[dict]:
        return [
            j for j in self.active_jobs.values()
            if j["status"] in ("completed", "failed")
        ]

    def get_running_count(self) -> int:
        return sum(1 for j in self.active_jobs.values() if j["status"] == "running")

    def has_active_or_queued(self) -> bool:
        return bool(self.active_jobs) or bool(self._retry_queue)

    def best_sharpe(self) -> float:
        best = 0.0
        for j in self.completed_results:
            s = j.get("metrics", {}).get("sharpe", 0) or 0
            if s > best:
                best = s
        for j in self.active_jobs.values():
            if j["status"] == "completed":
                s = j.get("metrics", {}).get("sharpe", 0) or 0
                if s > best:
                    best = s
        return best

    def meets_convergence_criteria(self) -> bool:
        all_results = list(self.completed_results)
        for j in self.active_jobs.values():
            if j["status"] == "completed":
                all_results.append(j)
        for j in all_results:
            m = j.get("metrics", {})
            sharpe = m.get("sharpe", 0) or 0
            fitness = m.get("fitness", 0) or 0
            turnover = m.get("turnover", 0) or 0
            checks = m.get("checks", [])
            all_pass = all(c.get("result") == "PASS" for c in checks)
            if (sharpe >= MIN_SHARPE
                    and fitness >= MIN_FITNESS
                    and MIN_TURNOVER < turnover < MAX_TURNOVER
                    and all_pass):
                return True
        return False

    def all_results(self) -> list[dict]:
        combined = list(self.completed_results)
        for j in self.active_jobs.values():
            combined.append(j)
        return combined

    def get_expression_history_stats(self) -> tuple[int, int]:
        total = len(self._expression_history)
        unique = len(set(self._expression_history))
        return total, unique

    # ── Job lifecycle ──────────────────────────────────────────────────────

    def create_job_record(self, expr: str, ex: dict, job_id: str, iteration: int):
        """Register a newly submitted job in active_jobs.

        Returns a dedup note string if a cross-round duplicate is detected,
        otherwise ``None``.  The caller should append the note to
        ``_dedup_notes`` on ``SubmissionEngine``.
        """
        norm = expr.replace(" ", "")
        self._expression_history.append(norm)

        # Cross-round dedup check.
        dedup_note = None
        for prev in self.completed_results:
            prev_expr = prev.get("expression", "").replace(" ", "")
            prev_round = prev.get("round", 0)
            if prev_expr == norm and prev_round != iteration:
                dedup_note = (
                    f"Note: same expression as round {prev_round} "
                    f"(previous result: Sharpe={prev.get('metrics', {}).get('sharpe', '?')}, "
                    f"Settings={prev.get('settings', {})})"
                )
                self._log_event("dedup_note", expression=expr[:100], previous_round=prev_round)
                break

        self.active_jobs[job_id] = {
            "job_id": job_id,
            "expression": expr,
            "settings": ex.get("settings", {}),
            "rationale": ex.get("rationale", ""),
            "direction_id": ex.get("direction_id", ""),
            "status": "running",
            "round": iteration,
            "submitted_at": datetime.now().isoformat(),
            "metrics": {},
            "error": "",
        }
        self._log_event("submit", expression=expr[:100], status="submitted", job_id=job_id)
        return dedup_note

    def mark_replaced(self, job_id: str | None) -> None:
        if job_id and job_id in self.active_jobs:
            self.active_jobs[job_id]["status"] = "replaced"

    def mark_abandoned_from(self, direction_ids: set[str]) -> None:
        """Mark active jobs whose direction has been abandoned."""
        for jid in self.active_jobs:
            if self.active_jobs[jid].get("direction_id") in direction_ids:
                self.active_jobs[jid]["status"] = "abandoned_from"

    def move_finished_to_completed(self, finished: list[dict]) -> None:
        done_ids = [j["job_id"] for j in finished]
        for jid in done_ids:
            if jid in self.active_jobs:
                self.completed_results.append(self.active_jobs.pop(jid))

    def move_remaining_to_completed(self) -> None:
        for jid in list(self.active_jobs.keys()):
            self.completed_results.append(self.active_jobs.pop(jid))

    # ── Retry queue ────────────────────────────────────────────────────────

    def move_to_retry(self, job_id: str, job: dict, error: str) -> None:
        job["status"] = "network_error"
        job["error"] = error
        self._retry_queue[job_id] = job
        del self.active_jobs[job_id]

    def recover_from_retry(self, job_id: str, job: dict, result: dict) -> None:
        job["status"] = "completed"
        if result.get("metrics"):
            job["metrics"] = result["metrics"]
        if result.get("alpha_id"):
            job["alpha_id"] = result["alpha_id"]
        job.pop("error", None)
        self.active_jobs[job_id] = job
        del self._retry_queue[job_id]

    def mark_retry_failed(self, job_id: str, job: dict, result: dict) -> None:
        job["status"] = "failed"
        job["error"] = result.get("error", "")
        self.completed_results.append(job)
        del self._retry_queue[job_id]

    # ── History ────────────────────────────────────────────────────────────

    def record_iteration(self, entry: dict) -> None:
        self._iteration_history.append(entry)
