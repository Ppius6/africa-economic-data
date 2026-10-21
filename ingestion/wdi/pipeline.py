"""Runs the WDI ingestion: fetch from the API, land in bronze."""
from config import AFRICA_ISO3
from db import connect

from .client import fetch_indicator, get_country_metadata, get_indicator_metadata
from .indicators import INDICATORS
from .loader import ensure_bronze_table, land_country_metadata, land_indicator, land_indicator_metadata


# Orchestration
def run():
    conn = connect()
    try:
        ensure_bronze_table(conn)
        print(f"Scope: {len(AFRICA_ISO3)} African countries (hardcoded list).")

        print("Fetching country metadata...")
        country_records = get_country_metadata(AFRICA_ISO3)
        land_country_metadata(conn, country_records)
        print(f"  landed {len(country_records)} country records")

        print("Fetching indicator metadata...")
        indicator_records = get_indicator_metadata(INDICATORS)
        land_indicator_metadata(conn, indicator_records)
        print(f"  landed {len(indicator_records)} indicator records")

        for indicator_code, name in INDICATORS.items():
            print(f"Fetching {indicator_code} ({name})...")
            records = fetch_indicator(indicator_code, AFRICA_ISO3)
            land_indicator(conn, indicator_code, records)
    finally:
        conn.close()

    print("Done.")

if __name__ == "__main__":
    run()