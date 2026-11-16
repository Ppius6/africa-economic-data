"""Runs the Comtrade ingestion: fetch from the API, land in the raw schema.

Usage:
    python -m comtrade.pipeline                  # START_YEAR to last year
    python -m comtrade.pipeline 2021 2023        # an inclusive range of years
"""
from datetime import date
import sys
import time

from config import AFRICA_ISO3, START_YEAR
from db import connect

from .client import (
    CALL_SPACING_SECONDS,
    ComtradeFetchError,
    fetch_trade,
    get_partner_areas,
    get_reporters,
    m49_codes_for,
)
from .loader import (
    PARTNER_SIDE,
    REPORTER_SIDE,
    IncompleteLoadError,
    ensure_raw_tables,
    land_partner_areas,
    land_reporters,
    land_trade,
)

REFRESH_WINDOW_YEARS = 5


def _refresh(conn, fetch, land) -> bool:
    """Fetch and land one slice. On failure, keep the slice's existing rows and move on."""
    try:
        records = fetch()
        land(conn, records)
        print(f"  landed {len(records)} records")
        return True
    except ComtradeFetchError as exc:
        print(f"  SKIPPED (API): {exc}")
    except IncompleteLoadError as exc:
        conn.rollback()
        print(f"  SKIPPED (incomplete): {exc}")
    return False


def default_years(today: date | None = None) -> range:
    last_year = (today or date.today()).year - 1
    first_year = max(START_YEAR, last_year - REFRESH_WINDOW_YEARS + 1)
    return range(first_year, last_year + 1)


def run(years: range | None = None):
    years = years or default_years()
    conn = connect()
    skipped = []
    try:
        ensure_raw_tables(conn)

        print("Fetching Comtrade reporter reference...")
        reporters = []

        def fetch_reporters():
            reporters.extend(get_reporters())
            return reporters

        if not _refresh(conn, fetch_reporters, land_reporters):
            skipped.append("reporter reference")
        if not reporters:
            raise RuntimeError(
                "No reporter reference, so the African M49 codes are unknown; stopping")

        print("Fetching Comtrade partner area reference...")
        if not _refresh(conn, get_partner_areas, land_partner_areas):
            skipped.append("partner area reference")

        codes = m49_codes_for(reporters, AFRICA_ISO3)
        print(
            f"Scope: {len(AFRICA_ISO3)} countries, {len(codes)} M49 codes, years {years.start}-{years.stop - 1}")

        for year in years:
            for side, kwargs in ((REPORTER_SIDE, {"reporter_codes": codes}), (PARTNER_SIDE, {"partner_codes": codes})):
                print(f"Fetching {year} {side} side...")
                if not _refresh(
                    conn,
                    lambda: fetch_trade(year, **kwargs),
                    lambda c, records: land_trade(c, side, year, records),
                ):
                    skipped.append(f"{side} {year}")
                time.sleep(CALL_SPACING_SECONDS)
    finally:
        conn.close()

    if skipped:
        raise RuntimeError(
            f"Raw tables not refreshed for {len(skipped)} slice(s): {', '.join(skipped)}")
    print("Done.")


if __name__ == "__main__":
    args = [int(a) for a in sys.argv[1:]]
    if len(args) == 2:
        run(range(args[0], args[1] + 1))
    elif not args:
        run()
    else:
        sys.exit("usage: python -m comtrade.pipeline [FROM_YEAR TO_YEAR]")
