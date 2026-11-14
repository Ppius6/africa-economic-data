"""UN Comtrade API client: fetches raw annual trade records, all products combined (HS TOTAL)."""

import os
import time

import requests
from dotenv import load_dotenv

load_dotenv()

COMTRADE_BASE_URL = "https://comtradeapi.un.org"
REPORTERS_URL = f"{COMTRADE_BASE_URL}/files/v1/app/reference/Reporters.json"
# Superset of the reporters: also areas that never report, such as "Areas, nes" (899) and "Bunkers" (837).
PARTNER_AREAS_URL = f"{COMTRADE_BASE_URL}/files/v1/app/reference/partnerAreas.json"
TRADE_URL = f"{COMTRADE_BASE_URL}/data/v1/get/C/A/HS"

MAX_RETRIES = 4
BACKOFF_BASE_SECONDS = 2
# (connect, read) seconds.
REQUEST_TIMEOUT = (10, 120)
# Free-tier ceiling per call. An answer this size may be cut off, so it is treated as an error.
MAX_RECORDS = 100_000
# Pause between data calls, to stay under the per-second rate limit.
CALL_SPACING_SECONDS = 1.5

# Codes that report for several of our countries at once, which ISO3 matching can't find.
# 711 is the Southern African Customs Union (ISO3 "ZA1"): until 1999 it reported jointly for
# South Africa, Botswana, Lesotho, Namibia and Eswatini, which only get codes of their own in 2000.
# Without it, most of their 1990s trade is missing from both sides of the pull.
GROUPED_PREDECESSOR_CODES = frozenset({711})

# Only the aggregate row: no second partner, all customs procedures, all transport modes.
AGGREGATE_ONLY = {"partner2Code": 0, "customsCode": "C00", "motCode": 0}


class ComtradeFetchError(RuntimeError):
    """The Comtrade API could not be reached, refused the request, or returned an incomplete answer."""


def _api_key() -> str:
    key = os.environ.get("COMTRADE_API_KEY", "").strip()
    if not key:
        raise ComtradeFetchError("COMTRADE_API_KEY is not set; add your Comtrade subscription key to .env")
    return key


def _retry_wait(response, attempt: int) -> float:
    """Seconds to wait before the next attempt. A 429 says how long in Retry-After."""
    if response is not None and response.status_code == 429:
        retry_after = response.headers.get("Retry-After", "")
        if retry_after.isdigit():
            return int(retry_after)
    return BACKOFF_BASE_SECONDS ** attempt


def fetch_with_retry(url: str, params: dict | None = None, headers: dict | None = None) -> dict:
    last_exception = None
    for attempt in range(1, MAX_RETRIES + 1):
        response = None
        try:
            response = requests.get(url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_exception = exc
            if attempt == MAX_RETRIES:
                print(f"  attempt {attempt}/{MAX_RETRIES} failed ({exc}); giving up")
                break
            wait = _retry_wait(response, attempt)
            print(f"  attempt {attempt}/{MAX_RETRIES} failed ({exc}); retrying in {wait}s")
            time.sleep(wait)
    raise ComtradeFetchError(f"Failed after {MAX_RETRIES} attempts: {url}") from last_exception


def get_reporters() -> list[dict]:
    """Comtrade's reporter reference: M49 code, ISO3 and validity dates for every reporter. Needs no key."""
    payload = fetch_with_retry(REPORTERS_URL)
    return payload.get("results") or []


def get_partner_areas() -> list[dict]:
    """Comtrade's partner reference: every code that can appear as a partner. Needs no key."""
    payload = fetch_with_retry(PARTNER_AREAS_URL)
    return payload.get("results") or []


def m49_codes_for(reporters: list[dict], iso3_codes: set[str]) -> list[int]:
    """Every M49 code Comtrade has used for these countries, plus the grouped predecessor codes.

    Some countries have more than one: Ethiopia is 230 up to 1992 and 231 after,
    Sudan is 736 up to mid-2011 and 729 after. Asking for all of them every year
    is safe, because Comtrade only returns data under the code valid that year.
    """
    by_iso3 = {
        r["reporterCode"]
        for r in reporters
        if r.get("reporterCodeIsoAlpha3") in iso3_codes and not r.get("isGroup")
    }
    return sorted(by_iso3 | GROUPED_PREDECESSOR_CODES)


def fetch_trade(year: int, *, reporter_codes: list[int] | None = None,
                partner_codes: list[int] | None = None) -> list[dict]:
    """Annual exports and imports, all products combined, for one year.

    Pass reporter_codes for what those countries report (against every partner),
    or partner_codes for what every country reports about those partners.
    Leaving reporterCode out means every reporter.
    """
    if (reporter_codes is None) == (partner_codes is None):
        raise ValueError("pass exactly one of reporter_codes or partner_codes")

    params = {
        "period": year,
        "flowCode": "X,M",
        "cmdCode": "TOTAL",
        "maxRecords": MAX_RECORDS,
        "includeDesc": "false",
        **AGGREGATE_ONLY,
    }
    if reporter_codes is not None:
        params["reporterCode"] = ",".join(map(str, reporter_codes))
    else:
        params["partnerCode"] = ",".join(map(str, partner_codes))

    payload = fetch_with_retry(TRADE_URL, params, headers={"Ocp-Apim-Subscription-Key": _api_key()})
    if payload.get("error"):
        raise ComtradeFetchError(f"Comtrade API error for {year}: {payload['error']}")

    records = payload.get("data") or []
    if len(records) >= MAX_RECORDS:
        raise ComtradeFetchError(
            f"{year}: answer hit the {MAX_RECORDS:,}-record cap and may be cut off; split the request"
        )
    return records
