"""This module runs the Comtrade HS-chapter ingestion: it fetches trade by product from the API and lands it in bronze.

Usage:
    python -m comtrade.product_pipeline              # This refreshes the last five complete years, never before 2000.
    python -m comtrade.product_pipeline 2021 2023    # This refreshes an inclusive range of years.
"""
from datetime import date
import sys

from config import AFRICA_ISO3
from db import connect

from .client import (
    fetch_in_batches,
    fetch_trade,
    get_hs_reference,
    get_reporters,
    hs_chapters,
    m49_codes_for,
)
from .loader import (
    PARTNER_SIDE,
    REPORTER_SIDE,
    ensure_bronze_tables,
    land_hs_reference,
    land_product_trade,
)
from .pipeline import REFRESH_WINDOW_YEARS, _refresh

# Before 2000 only a handful of African countries reported to Comtrade themselves,
# so chapter-level data would be almost entirely mirror figures.
PRODUCT_START_YEAR = 2000

REPORTER_BATCH_SIZE = 5
# all reporters' records about the 54 are ~2k rows per chapter
CHAPTER_BATCH_SIZE = 25


def default_years(today: date | None = None) -> range:
    last_year = (today or date.today()).year - 1
    first_year = max(PRODUCT_START_YEAR, last_year - REFRESH_WINDOW_YEARS + 1)
    return range(first_year, last_year + 1)


def run(years: range | None = None):
    years = years or default_years()
    conn = connect()
    skipped = []
    try:
        ensure_bronze_tables(conn)

        print("Fetching Comtrade reporter and HS reference...")
        reporters = get_reporters()
        hs_reference = get_hs_reference()
        if not reporters or not hs_reference:
            raise RuntimeError(
                "Reporter or HS reference came back empty; stopping")

        if not _refresh(conn, lambda: hs_reference, land_hs_reference):
            skipped.append("HS reference")

        codes = m49_codes_for(reporters, AFRICA_ISO3)
        chapters = hs_chapters(hs_reference)
        print(f"Scope: {len(codes)} M49 codes, {len(chapters)} HS chapters, "
              f"years {years.start}-{years.stop - 1}")

        for year in years:
            fetches = {
                # Every chapter fits in one call per reporter batch, so split by reporter.
                REPORTER_SIDE: lambda: fetch_in_batches(
                    lambda batch: fetch_trade(
                        year, reporter_codes=batch, cmd_codes="AG2"),
                    codes, REPORTER_BATCH_SIZE
                ),
                # Split by chapter
                PARTNER_SIDE: lambda: fetch_in_batches(
                    lambda batch: fetch_trade(
                        year, partner_codes=codes, cmd_codes=",".join(batch)),
                    chapters, CHAPTER_BATCH_SIZE
                ),
            }
            for side, fetch in fetches.items():
                print(f"Fetching {year} {side} side by chapter...")
                if not _refresh(
                    conn,
                    fetch,
                    lambda c, records: land_product_trade(
                        c, side, year, records),
                ):
                    skipped.append(f"{side} {year}")
    finally:
        conn.close()

    if skipped:
        raise RuntimeError(
            f"Product Bronze not refreshed for {len(skipped)} slice(s): {', '.join(skipped)}")
    print("Done.")


if __name__ == "__main__":
    args = [int(a) for a in sys.argv[1:]]
    if len(args) == 2:
        run(range(args[0], args[1] + 1))
    elif not args:
        run()
    else:
        sys.exit(
            "usage: python -m comtrade.product_pipeline [FROM_YEAR TO_YEAR]")
