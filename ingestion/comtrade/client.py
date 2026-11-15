"""This module fetches raw annual trade records and reference files from the UN Comtrade API.

Trade can be requested for all products combined (HS TOTAL) or for chosen commodity codes, such as HS chapters.
"""

import os
import time

import requests
from dotenv import load_dotenv

load_dotenv()

COMTRADE_BASE_URL = "https://comtradeapi.un.org"
REPORTERS_URL = f"{COMTRADE_BASE_URL}/files/v1/app/reference/Reporters.json"
PARTNER_AREAS_URL = f"{COMTRADE_BASE_URL}/files/v1/app/reference/partnerAreas.json"
TRADE_URL = f"{COMTRADE_BASE_URL}/data/v1/get/C/A/HS"
HS_REFERENCE_URL = f"{COMTRADE_BASE_URL}/files/v1/app/reference/HS.json"

MAX_RETRIES = 4
BACKOFF_BASE_SECONDS = 2
REQUEST_TIMEOUT = (10, 120)  # (connect, read) seconds.
MAX_RECORDS = 100_000
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


class RecordCapError(ComtradeFetchError):
    """The answer reached MAX_RECORDS and may be cut off; the request needs splitting."""


def _api_key() -> str:
    key = os.environ.get("COMTRADE_API_KEY", "").strip()
    if not key:
        raise ComtradeFetchError(
            "COMTRADE_API_KEY is not set; add your Comtrade subscription key to .env")
    return key


def _retry_wait(response, attempt: int) -> float:
    """Returns the seconds to wait before the next attempt. A 429 answer says how long in its Retry-After header."""
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
            response = requests.get(
                url, params=params, headers=headers, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_exception = exc
            if attempt == MAX_RETRIES:
                print(
                    f"  attempt {attempt}/{MAX_RETRIES} failed ({exc}); giving up")
                break
            wait = _retry_wait(response, attempt)
            print(
                f"  attempt {attempt}/{MAX_RETRIES} failed ({exc}); retrying in {wait}s")
            time.sleep(wait)
    raise ComtradeFetchError(
        f"Failed after {MAX_RETRIES} attempts: {url}") from last_exception


def get_reporters() -> list[dict]:
    """Returns Comtrade's reporter reference, which gives the M49 code, ISO3 code and validity dates of every reporter.

    This call needs no subscription key.
    """
    payload = fetch_with_retry(REPORTERS_URL)
    return payload.get("results") or []


def get_partner_areas() -> list[dict]:
    """Returns Comtrade's partner reference, which lists every code that can appear as a partner.

    This call needs no subscription key.
    """
    payload = fetch_with_retry(PARTNER_AREAS_URL)
    return payload.get("results") or []


def m49_codes_for(reporters: list[dict], iso3_codes: set[str]) -> list[int]:
    """Returns every M49 code Comtrade has used for these countries, plus the grouped predecessor codes.

    Some countries have more than one: Ethiopia is 230 up to 1992 and 231 after,
    Sudan is 736 up to mid-2011 and 729 after. Asking for all of them every year is safe,
    because Comtrade only returns data under the code that was valid in that year.
    """
    by_iso3 = {
        r["reporterCode"]
        for r in reporters
        if r.get("reporterCodeIsoAlpha3") in iso3_codes and not r.get("isGroup")
    }
    return sorted(by_iso3 | GROUPED_PREDECESSOR_CODES)


def fetch_trade(year: int, *, reporter_codes: list[int] | None = None,
                partner_codes: list[int] | None = None, cmd_codes: str = "TOTAL") -> list[dict]:
    """Returns one year of annual exports and imports for the given commodity codes.

    Pass reporter_codes to get what those countries report against every partner, or
    partner_codes to get what every country reports about those partners. When only
    partner_codes is given, the request leaves reporterCode out, which means every reporter.

    cmd_codes is passed to the API unchanged. The default "TOTAL" means all products combined,
    "AG2" means every HS chapter, and a comma-separated list such as "01,02" means those codes only.
    An answer that reaches MAX_RECORDS raises RecordCapError, because it may have been cut off.
    """
    if (reporter_codes is None) == (partner_codes is None):
        raise ValueError("pass exactly one of reporter_codes or partner_codes")

    params = {
        "period": year,
        "flowCode": "X,M",
        "cmdCode": cmd_codes,
        "maxRecords": MAX_RECORDS,
        "includeDesc": "false",
        **AGGREGATE_ONLY,
    }
    if reporter_codes is not None:
        params["reporterCode"] = ",".join(map(str, reporter_codes))
    else:
        params["partnerCode"] = ",".join(map(str, partner_codes))

    payload = fetch_with_retry(TRADE_URL, params, headers={
                               "Ocp-Apim-Subscription-Key": _api_key()})
    if payload.get("error"):
        raise ComtradeFetchError(
            f"Comtrade API error for {year}: {payload['error']}")

    records = payload.get("data") or []

    if len(records) >= MAX_RECORDS:
        raise RecordCapError(
            f"{year}: answer hit the {MAX_RECORDS:,}-record cap and may be cut off")

    return records


def get_hs_reference() -> list[dict]:
    """Returns Comtrade's HS commodity reference, which covers every level from TOTAL down to 6-digit codes.

    This call needs no subscription key.
    """
    payload = fetch_with_retry(HS_REFERENCE_URL)
    return payload.get("results") or []


def hs_chapters(hs_reference: list[dict]) -> list[str]:
    """Returns the sorted 2-digit HS chapter codes from the reference.

    The list includes chapter 99 (commodities not specified), which a hard-coded range of 01 to 97 would miss.
    """
    return sorted(r["id"] for r in hs_reference if r.get("aggrLevel") == 2)


def fetch_in_batches(fetch_batch, items: list, batch_size: int) -> list[dict]:
    """Calls fetch_batch on each batch of items and returns all the answers combined.

    A batch whose answer hits the record cap is split in half and both halves are retried,
    so the batch size is a starting guess, not something that has to be tuned exactly.
    """
    records = []
    pending = [items[i:i + batch_size]
               for i in range(0, len(items), batch_size)]
    while pending:
        batch = pending.pop()
        try:
            records.extend(fetch_batch(batch))
        except RecordCapError:
            if len(batch) == 1:
                raise
            middle = len(batch) // 2
            print(
                f"  batch of {len(batch)} hit the record cap; splitting in half")
            pending += [batch[:middle], batch[middle:]]
        time.sleep(CALL_SPACING_SECONDS)
    return records
