"""
World Bank WDI API Client: fetches raw records.
"""

from datetime import date
import time

import requests

from config import START_YEAR

# Config
WDI_BASE_URL = "https://api.worldbank.org/v2"
MAX_RETRIES = 4
BACKOFF_BASE_SECONDS = 2
# (connect, read) seconds. WDI can take 30s+ to answer a query it hasn't cached,
# then answers the same query in under a second, so the read timeout is generous.
REQUEST_TIMEOUT = (10, 120)


class WDIFetchError(RuntimeError):
    """The WDI API could not be reached or returned an error."""


def fetch_with_retry(url: str, params: dict) -> dict:
    last_exception = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_exception = exc
            if attempt == MAX_RETRIES:
                print(f"  attempt {attempt}/{MAX_RETRIES} failed ({exc}); giving up")
                break
            wait = BACKOFF_BASE_SECONDS ** attempt
            print(
                f"  attempt {attempt}/{MAX_RETRIES} failed ({exc}); retrying in {wait}s")
            time.sleep(wait)
    raise WDIFetchError(
        f"Failed after {MAX_RETRIES} attempts: {url}") from last_exception


# Country metadata
def get_country_metadata(iso3_codes: set[str]) -> list[dict]:
    """Fetch name/region/income/capital/lat-long for the given countries.
    Used to seed dim_country downstream; plays no role in deciding scope."""
    country_param = ";".join(sorted(iso3_codes))
    url = f"{WDI_BASE_URL}/country/{country_param}"
    params = {"format": "json", "per_page": 400}
    payload = fetch_with_retry(url, params)
    records = payload[1] if payload and payload[0] else []

    found = {c["id"] for c in records}
    missing = iso3_codes - found
    if missing:
        print(f"WARNING: no WDI country record found for: {missing}")

    return records

# Indicator metadata


def get_indicator_metadata(indicator_codes: dict) -> list[dict]:
    all_records = []
    for code in indicator_codes.keys():
        url = f"{WDI_BASE_URL}/indicator/{code}"
        params = {"format": "json"}
        payload = fetch_with_retry(url, params)
        if payload and len(payload) > 1 and payload[1]:
            all_records.extend(payload[1])
    return all_records

# Indicator pull (paginated)


def fetch_indicator(indicator_code: str, country_codes: set[str]) -> list[dict]:
    country_param = ";".join(sorted(country_codes))
    url = f"{WDI_BASE_URL}/country/{country_param}/indicator/{indicator_code}"

    all_records = []
    page = 1
    while True:
        params = {"format": "json", "per_page": 20000,
                  "date": f"{START_YEAR}:{date.today().year}", "page": page}
        payload = fetch_with_retry(url, params)

        if payload and isinstance(payload[0], dict) and "message" in payload[0]:
            raise WDIFetchError(f"WDI API error for {indicator_code}: {payload[0]['message']}")

        if not payload or payload[0] is None:
            break  # WDI returns [None] for an invalid/empty query

        meta, records = payload[0], payload[1] or []
        all_records.extend(records)

        if page >= meta["pages"]:
            break
        page += 1

    print(f"  {indicator_code}: {len(all_records)} records across {page} page(s)")
    return all_records
