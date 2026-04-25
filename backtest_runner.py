import argparse
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from os import environ
from pathlib import Path

import requests
from requests import Session
from requests.auth import HTTPBasicAuth


API_BASE = "https://api.worldquantbrain.com"
DEFAULT_INPUT_DIR = "factor_batches"
DEFAULT_OUTPUT_DIR = "backtest_results"
DEFAULT_SCAN_INTERVAL_SECONDS = 15
DEFAULT_MAX_WORKERS = 3
DEFAULT_MAX_RETRIES = 3
DEFAULT_RETRY_SLEEP_SECONDS = 2
DEFAULT_LOG_DIRNAME = "logs"
DEFAULT_RELOGIN_INTERVAL_SECONDS = 13800


def setup_logger(output_dir: Path, log_level: str = "INFO") -> logging.Logger:
    """Create a logger that writes both to console and a timestamped file."""

    logger = logging.getLogger("backtest_runner")
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))

    if logger.handlers:
        return logger

    log_dir = output_dir / DEFAULT_LOG_DIRNAME
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"backtest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.info("Backtest logging initialized. Log file: %s", log_file)
    return logger


def load_credentials() -> tuple[str, str]:
    """Load API credentials from local .env first, then process environment variables."""

    def parse_dotenv(dotenv_path: Path) -> dict[str, str]:
        values: dict[str, str] = {}
        if not dotenv_path.exists():
            return values

        with open(dotenv_path, encoding="utf-8") as file_handle:
            for raw_line in file_handle:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                clean_key = key.strip()
                clean_value = value.strip().strip('"').strip("'")
                values[clean_key] = clean_value
        return values

    dotenv_path = Path(__file__).resolve().parent / ".env"
    dotenv_values = parse_dotenv(dotenv_path)

    username = dotenv_values.get("BRAIN_USERNAME") or environ.get("BRAIN_USERNAME")
    password = dotenv_values.get("BRAIN_PASSWORD") or environ.get("BRAIN_PASSWORD")

    if not username or not password:
        raise RuntimeError(
            "Missing credentials. Set BRAIN_USERNAME/BRAIN_PASSWORD in .env or environment variables."
        )
    return username, password


def create_authenticated_session(username: str, password: str) -> Session:
    """Create a requests session and verify authentication once up front."""

    session = requests.Session()
    session.auth = HTTPBasicAuth(username, password)

    response = session.post(f"{API_BASE}/authentication", timeout=30)
    response.raise_for_status()
    return session


class BrainSessionManager:
    """Manage authenticated session lifecycle with periodic relogin."""

    def __init__(
        self,
        username: str,
        password: str,
        logger: logging.Logger,
        relogin_interval_seconds: int,
    ):
        self.username = username
        self.password = password
        self.logger = logger
        self.relogin_interval_seconds = max(1, relogin_interval_seconds)
        self._lock = threading.Lock()
        self._session: Session | None = None
        self._last_login_monotonic = 0.0

    def _needs_relogin(self) -> bool:
        if self._session is None:
            return True
        elapsed = time.monotonic() - self._last_login_monotonic
        return elapsed >= self.relogin_interval_seconds

    def _login(self) -> Session:
        session = create_authenticated_session(self.username, self.password)
        self._session = session
        self._last_login_monotonic = time.monotonic()
        self.logger.info("Session login refreshed successfully.")
        return session

    def get_session(self, force_relogin: bool = False) -> Session:
        with self._lock:
            if force_relogin or self._needs_relogin():
                return self._login()
            return self._session

    def request(self, method: str, url: str, **kwargs) -> requests.Response:
        session = self.get_session()
        response = session.request(method, url, **kwargs)

        # Retry once with forced relogin only when auth is explicitly rejected.
        if response.status_code in (401, 403):
            self.logger.warning("Received %s, forcing relogin and retrying request.", response.status_code)
            session = self.get_session(force_relogin=True)
            response = session.request(method, url, **kwargs)
        return response


def run_single_backtest(
    session_manager: BrainSessionManager,
    factor_payload: dict,
    max_retries: int,
    retry_sleep_seconds: float,
    logger: logging.Logger,
) -> dict:
    """Submit one factor, retry failures, and return simulation/alpha details."""

    last_error = ""
    for attempt in range(1, max_retries + 1):
        try:
            logger.debug("Submitting factor attempt %s/%s", attempt, max_retries)
            submit_resp = session_manager.request("POST", f"{API_BASE}/simulations", json=factor_payload, timeout=30)
            submit_resp.raise_for_status()

            progress_url = submit_resp.headers.get("Location")
            if not progress_url:
                raise ValueError("Missing progress Location header")

            while True:
                progress_resp = session_manager.request("GET", progress_url, timeout=30)
                progress_resp.raise_for_status()
                retry_after_sec = float(progress_resp.headers.get("Retry-After", 0))
                if retry_after_sec == 0:
                    break
                time.sleep(retry_after_sec)

            simulation_summary = progress_resp.json()
            alpha_id = simulation_summary.get("alpha")

            alpha_detail = {}
            if alpha_id:
                # Fetch alpha detail to preserve performance metrics for later screening.
                alpha_resp = session_manager.request("GET", f"{API_BASE}/alphas/{alpha_id}", timeout=30)
                alpha_resp.raise_for_status()
                alpha_detail = alpha_resp.json()

            logger.info("Factor backtest success. alpha_id=%s attempts=%s", alpha_id, attempt)

            return {
                "status": "ok",
                "error": "",
                "alpha_id": alpha_id,
                "simulation_summary": simulation_summary,
                "alpha_detail": alpha_detail,
                "attempts": attempt,
            }
        except (requests.RequestException, ValueError) as exc:
            last_error = str(exc)
            logger.warning("Factor backtest attempt %s/%s failed: %s", attempt, max_retries, last_error)
            if attempt < max_retries:
                time.sleep(max(0, retry_sleep_seconds))

    # Reached retry limit: skip this factor and continue others.
    return {
        "status": "skipped_after_retries",
        "error": f"Failed after {max_retries} attempts: {last_error}",
        "alpha_id": None,
        "simulation_summary": {},
        "alpha_detail": {},
        "attempts": max_retries,
    }


def process_single_factor(
    session_manager: BrainSessionManager,
    factor: dict,
    index: int,
    max_retries: int,
    retry_sleep_seconds: float,
    logger: logging.Logger,
) -> dict:
    """Run one factor backtest and normalize output with index/expression/settings."""

    single_result = run_single_backtest(
        session_manager=session_manager,
        factor_payload=factor,
        max_retries=max_retries,
        retry_sleep_seconds=retry_sleep_seconds,
        logger=logger,
    )
    single_result["index"] = index
    single_result["regular"] = factor.get("regular", "")
    single_result["settings"] = factor.get("settings", {})
    return single_result


def process_batch_file(
    session_manager: BrainSessionManager,
    input_file: Path,
    output_file: Path,
    max_workers: int,
    max_retries: int,
    retry_sleep_seconds: float,
    logger: logging.Logger,
) -> None:
    """Read one generated factor batch and write corresponding backtest results."""

    with open(input_file, encoding="utf-8") as file_handle:
        batch_payload = json.load(file_handle)

    factors = batch_payload.get("factors", [])
    results = [None] * len(factors)

    logger.info("Processing batch: %s (%s factors)", input_file, len(factors))

    completed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_index = {
            executor.submit(
                process_single_factor,
                session_manager,
                factor,
                index,
                max_retries,
                retry_sleep_seconds,
                logger,
            ): index
            for index, factor in enumerate(factors, start=1)
        }

        for future in as_completed(future_to_index):
            index = future_to_index[future]
            single_result = future.result()
            results[index - 1] = single_result
            completed += 1

            if single_result["status"] == "skipped_after_retries":
                logger.warning("Factor %s skipped after retries: %s", index, single_result["error"])

            if completed % 10 == 0 or completed == len(factors):
                logger.info("Batch progress %s/%s", completed, len(factors))

    output_payload = {
        "source_batch": str(input_file),
        "processed_at": datetime.now().isoformat(timespec="seconds"),
        "dataset_id": batch_payload.get("dataset_id", ""),
        "input_count": len(factors),
        "result_count": len([item for item in results if item is not None]),
        "results": results,
    }

    output_file.parent.mkdir(parents=True, exist_ok=True)
    with open(output_file, "w", encoding="utf-8") as file_handle:
        json.dump(output_payload, file_handle, ensure_ascii=False, indent=2)

    ok_count = len([item for item in results if item and item.get("status") == "ok"])
    skipped_count = len([item for item in results if item and item.get("status") == "skipped_after_retries"])
    logger.info(
        "Wrote backtest results: %s (ok=%s skipped_after_retries=%s)",
        output_file,
        ok_count,
        skipped_count,
    )


def iter_unprocessed_batches(input_dir: Path, output_dir: Path):
    """Yield generated batch files that do not have corresponding result files yet."""

    for input_file in sorted(input_dir.rglob("*.json")):
        relative_path = input_file.relative_to(input_dir)
        output_file = output_dir / relative_path
        if output_file.exists():
            continue
        yield input_file, output_file


def main() -> None:
    parser = argparse.ArgumentParser(description="Continuously backtest generated factors from local batch files.")
    parser.add_argument("--input-dir", default=DEFAULT_INPUT_DIR, help="Directory of generated factor batch files.")
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR, help="Directory for mirrored backtest result files.")
    parser.add_argument(
        "--scan-interval",
        type=int,
        default=DEFAULT_SCAN_INTERVAL_SECONDS,
        help="Idle sleep seconds between directory scans.",
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=DEFAULT_MAX_WORKERS,
        help="Parallel backtest worker count. WorldQuant Brain supports up to 3 concurrent simulations.",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=DEFAULT_MAX_RETRIES,
        help="Maximum retry attempts for a single factor before skipping it.",
    )
    parser.add_argument(
        "--retry-sleep",
        type=float,
        default=DEFAULT_RETRY_SLEEP_SECONDS,
        help="Sleep seconds between retry attempts for the same factor.",
    )
    parser.add_argument(
        "--relogin-interval-seconds",
        type=int,
        default=DEFAULT_RELOGIN_INTERVAL_SECONDS,
        help="Refresh login session every N seconds (default 13800 ~= 3h50m).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity level.",
    )
    parser.add_argument("--once", action="store_true", help="Run one scan and exit instead of daemon mode.")
    args = parser.parse_args()

    max_workers = min(3, max(1, args.max_workers))
    max_retries = max(1, args.max_retries)
    retry_sleep_seconds = max(0.0, args.retry_sleep)
    relogin_interval_seconds = max(1, args.relogin_interval_seconds)

    input_dir = Path(args.input_dir)
    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    logger = setup_logger(output_dir=output_dir, log_level=args.log_level)

    username, password = load_credentials()
    session_manager = BrainSessionManager(
        username=username,
        password=password,
        logger=logger,
        relogin_interval_seconds=relogin_interval_seconds,
    )
    session_manager.get_session(force_relogin=True)
    logger.info(
        "Runner started. input_dir=%s output_dir=%s max_workers=%s max_retries=%s retry_sleep=%s relogin_interval_seconds=%s once=%s",
        input_dir,
        output_dir,
        max_workers,
        max_retries,
        retry_sleep_seconds,
        relogin_interval_seconds,
        args.once,
    )

    while True:
        pending = list(iter_unprocessed_batches(input_dir=input_dir, output_dir=output_dir))
        if not pending:
            logger.info("No new factor batch found under: %s", input_dir)
            if args.once:
                break
            time.sleep(max(1, args.scan_interval))
            continue

        logger.info("Found %s pending batch file(s).", len(pending))
        for input_file, output_file in pending:
            process_batch_file(
                session_manager=session_manager,
                input_file=input_file,
                output_file=output_file,
                max_workers=max_workers,
                max_retries=max_retries,
                retry_sleep_seconds=retry_sleep_seconds,
                logger=logger,
            )

        if args.once:
            break


if __name__ == "__main__":
    main()
