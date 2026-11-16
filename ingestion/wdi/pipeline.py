"""Runs the WDI ingestion: fetch from the API, land in the raw schema."""
from config import AFRICA_ISO3
from db import connect

from .client import WDIFetchError, fetch_indicator, get_country_metadata, get_indicator_metadata
from .indicators import INDICATORS
from .loader import (
    IncompleteLoadError,
    ensure_raw_tables,
    land_country_metadata,
    land_indicator,
    land_indicator_metadata,
)


def _refresh(conn, fetch, land) -> bool:
    """Fetch and land one slice of the raw tables.

    If the API fails or the data is incomplete, the existing rows for that slice
    stay as they are and the run moves on to the next one.
    """
    try:
        records = fetch()
        land(conn, records)
        print(f"  landed {len(records)} records")
        return True
    except WDIFetchError as exc:
        print(f"  SKIPPED (API): {exc}")
    except IncompleteLoadError as exc:
        conn.rollback()
        print(f"  SKIPPED (incomplete): {exc}")
    return False


# Orchestration
def run():
    conn = connect()
    skipped = []
    try:
        ensure_raw_tables(conn)
        print(f"Scope: {len(AFRICA_ISO3)} African countries (hardcoded list).")

        print("Fetching country metadata...")
        if not _refresh(conn,
                        lambda: get_country_metadata(AFRICA_ISO3), land_country_metadata):
            skipped.append("country metadata")

        print("Fetching indicator metadata...")
        if not _refresh(conn,
                        lambda: get_indicator_metadata(INDICATORS), land_indicator_metadata):
            skipped.append("indicator metadata")

        for indicator_code, name in INDICATORS.items():
            print(f"Fetching {indicator_code} ({name})...")
            if not _refresh(
                conn,
                lambda: fetch_indicator(indicator_code, AFRICA_ISO3),
                lambda c, records: land_indicator(c, indicator_code, records),
            ):
                skipped.append(indicator_code)
    finally:
        conn.close()

    if skipped:
        raise RuntimeError(
            f"Raw tables not refreshed for {len(skipped)} of {len(INDICATORS) + 2}: {', '.join(skipped)}"
        )
    print("Done.")

if __name__ == "__main__":
    run()
