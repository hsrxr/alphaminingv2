import json
import argparse
import time
from datetime import datetime
from os import environ
from pathlib import Path

import pandas as pd
import requests
from requests import Session
from requests.auth import HTTPBasicAuth



API_BASE = "https://api.worldquantbrain.com"
DATA_FIELDS_PAGE_SIZE = 50
DATA_FIELDS_SLEEP_SECONDS = 5


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
    print(response.status_code)
    print(response.json())
    return session


def fetch_and_store_datafields(
    dataset_id: str,
    output_dir: Path | str,
    instrument_type: str = "EQUITY",
    region: str = "USA",
    delay: int = 1,
    universe: str = "TOP3000",
    data_type: str = "MATRIX",
    search: str = "",
) -> pd.DataFrame:
    """Fetch all data fields, store each API page locally, and return the merged dataframe."""

    def load_cached_datafields(cache_root: Path, target_dataset_id: str) -> pd.DataFrame | None:
        """Load the newest cached run for a dataset if local cache files exist."""

        dataset_dir = cache_root / target_dataset_id
        if not dataset_dir.exists() or not dataset_dir.is_dir():
            return None

        # Each run is stored under dataset_id/YYYYMMDD_HHMMSS; use the latest run.
        run_dirs = sorted([path for path in dataset_dir.iterdir() if path.is_dir()])
        if not run_dirs:
            return None

        latest_run_dir = run_dirs[-1]
        page_files = sorted(latest_run_dir.glob("page_*.json"))
        if not page_files:
            return None

        merged_results = []
        for page_file in page_files:
            with open(page_file, encoding="utf-8") as file_handle:
                page_payload = json.load(file_handle)
            merged_results.extend(page_payload.get("results", []))

        if not merged_results:
            return None

        print(f"Loaded {len(merged_results)} data fields from local cache: {latest_run_dir}")
        return pd.DataFrame(merged_results)

    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    cached_df = load_cached_datafields(output_dir, dataset_id)
    if cached_df is not None:
        return cached_df

    username, password = load_credentials()
    session = create_authenticated_session(username, password)

    run_dir = output_dir / dataset_id / datetime.now().strftime("%Y%m%d_%H%M%S")
    run_dir.mkdir(parents=True, exist_ok=True)

    offset = 0
    page_index = 1
    datafields_list = []

    while True:
        params = {
            "instrumentType": instrument_type,
            "region": region,
            "delay": delay,
            "universe": universe,
            "dataset.id": dataset_id,
            "limit": DATA_FIELDS_PAGE_SIZE,
            "offset": offset,
            "type": data_type,
        }
        if search:
            params["search"] = search

        response = session.get(f"{API_BASE}/data-fields", params=params, timeout=30)
        response.raise_for_status()
        results = response.json()

        if "results" not in results:
            print(f"Unexpected response: {results}")
            break

        current_batch = results["results"]
        datafields_list.extend(current_batch)

        page_file = run_dir / f"page_{page_index:04d}.json"
        page_payload = {
            "dataset_id": dataset_id,
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "page_index": page_index,
            "offset": offset,
            "count": len(current_batch),
            "request": {
                "instrumentType": instrument_type,
                "region": region,
                "delay": delay,
                "universe": universe,
                "data_type": data_type,
                "search": search,
            },
            "results": current_batch,
        }
        with open(page_file, "w", encoding="utf-8") as file_handle:
            json.dump(page_payload, file_handle, ensure_ascii=False, indent=2)
        print(f"Fetched and stored {len(current_batch)} data fields at {page_file}")

        if len(current_batch) < DATA_FIELDS_PAGE_SIZE:
            print("Fetched the last batch of data fields.")
            break

        offset += DATA_FIELDS_PAGE_SIZE
        page_index += 1
        time.sleep(DATA_FIELDS_SLEEP_SECONDS)

    return pd.DataFrame(datafields_list)


def main() -> None:
    """Allow this module to be run directly for standalone data-field downloads."""

    parser = argparse.ArgumentParser(description="Fetch Brain data fields and store each page locally.")
    parser.add_argument("--dataset-id", required=True, help="Dataset id to query, for example pv13.")
    parser.add_argument("--output-dir", default="datafields_cache", help="Directory used to store fetched pages.")
    parser.add_argument("--instrument-type", default="EQUITY", help="Instrument type filter.")
    parser.add_argument("--region", default="USA", help="Region filter.")
    parser.add_argument("--delay", type=int, default=1, help="Delay filter.")
    parser.add_argument("--universe", default="TOP3000", help="Universe filter.")
    parser.add_argument("--data-type", default="MATRIX", help="Data field type filter.")
    parser.add_argument("--search", default="", help="Optional search keyword.")
    args = parser.parse_args()

    dataframe = fetch_and_store_datafields(
        dataset_id=args.dataset_id,
        output_dir=Path(args.output_dir),
        instrument_type=args.instrument_type,
        region=args.region,
        delay=args.delay,
        universe=args.universe,
        data_type=args.data_type,
        search=args.search,
    )
    print(f"Fetched {len(dataframe)} data fields in total.")


if __name__ == "__main__":
    main()
