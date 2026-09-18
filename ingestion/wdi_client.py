"""
World Bank WDI ingestion - lands raw indicator data for African countries
into bronze.wdi_raw. 
"""
import json
import time
from datetime import datetime, timezone

import psycopg2
import requests

# Config

WDI_BASE_URL = "https://api.worldbank.org/v2"

INDICATORS = {
    # Macro / growth
    "NY.GDP.MKTP.CD": "GDP (current US$)",
    "NY.GDP.PCAP.CD": "GDP per capita (current US$)",
    "NY.GDP.MKTP.KD.ZG": "GDP growth (annual %)",
    "GC.DOD.TOTL.GD.ZS": "Central government debt (% of GDP)",
    "BX.KLT.DINV.WD.GD.ZS": "Foreign direct investment, net inflows (% of GDP)",

    # Trade & external balance
    "NE.EXP.GNFS.ZS": "Exports of goods and services (% of GDP)",
    "NE.IMP.GNFS.ZS": "Imports of goods and services (% of GDP)",
    "BN.CAB.XOKA.GD.ZS": "Current account balance (% of GDP)",

    # Demographics & poverty
    "SP.POP.TOTL": "Population, total",
    "SP.DYN.LE00.IN": "Life expectancy at birth",
    "SP.URB.TOTL.IN.ZS": "Urban population (% of total)",
    "SI.POV.GINI": "GINI index",

    # Labor
    "SL.UEM.TOTL.ZS": "Unemployment, total (% of labor force)",

    # Prices
    "FP.CPI.TOTL.ZG": "Inflation, consumer prices (annual %)",

    # Health
    "SH.DYN.MORT": "Under-5 mortality rate",
    "SH.XPD.CHEX.GD.ZS": "Health expenditure (% of GDP)",
    "SH.STA.MMRT": "Maternal mortality ratio",

    # Education
    "SE.PRM.CMPT.ZS": "Primary completion rate",
    "SE.ADT.LITR.ZS": "Adult literacy rate",

    # Infrastructure & technology
    "IT.NET.USER.ZS": "Individuals using the internet (% of population)",
    "IT.CEL.SETS.P2": "Mobile cellular subscriptions (per 100 people)",
    "EG.ELC.ACCS.ZS": "Access to electricity (% of population)",
    "EG.FEC.RNEW.ZS": "Renewable energy consumption (% of total)",
    "EN.GHG.CO2.PC.CE.AR5": "CO2 emissions per capita",

    # Agriculture
    "NV.AGR.TOTL.ZS": "Agriculture, value added (% of GDP)",
    "AG.LND.AGRI.ZS": "Agricultural land (% of land area)",
}

AFRICA_ISO3 = {
    "DZA", "AGO", "BEN", "BWA", "BFA", "BDI", "CPV", "CMR", "CAF", "TCD",
    "COM", "COD", "COG", "CIV", "DJI", "EGY", "GNQ", "ERI", "SWZ", "ETH",
    "GAB", "GMB", "GHA", "GIN", "GNB", "KEN", "LSO", "LBR", "LBY", "MDG",
    "MWI", "MLI", "MRT", "MUS", "MAR", "MOZ", "NAM", "NER", "NGA", "RWA",
    "STP", "SEN", "SYC", "SLE", "SOM", "ZAF", "SSD", "SDN", "TZA", "TGO",
    "TUN", "UGA", "ZMB", "ZWE",
}


DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "user": "dev",
    "password": "dev",
    "dbname": "worldbank",
}

MAX_RETRIES = 4
BACKOFF_BASE_SECONDS = 2


# HTTP with retry/backoff

def fetch_with_retry(url: str, params: dict) -> dict:
    last_exception = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_exception = exc
            wait = BACKOFF_BASE_SECONDS ** attempt
            print(f"  attempt {attempt}/{MAX_RETRIES} failed ({exc}); retrying in {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"Failed after {MAX_RETRIES} attempts: {url}") from last_exception


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
        params = {"format": "json", "per_page": 20000, "date": "1990:2025", "page": page}
        payload = fetch_with_retry(url, params)

        if not payload or payload[0] is None:
            break  # WDI returns [None] for an invalid/empty query

        meta, records = payload[0], payload[1] or []
        all_records.extend(records)

        if page >= meta["pages"]:
            break
        page += 1

    print(f"  {indicator_code}: {len(all_records)} records across {page} page(s)")
    return all_records


# Bronze landing (full replace per indicator)

def ensure_bronze_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS bronze;
            CREATE TABLE IF NOT EXISTS bronze.wdi_raw (
                indicator_code TEXT NOT NULL,
                raw_record JSONB NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL
            );
            CREATE TABLE IF NOT EXISTS bronze.country_raw (
                country_code TEXT NOT NULL,
                raw_record JSONB NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL
            );
            CREATE TABLE IF NOT EXISTS bronze.indicator_raw (
                indicator_code TEXT NOT NULL,
                raw_record JSONB NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL
            );
        """)
    conn.commit()

def land_country_metadata(conn, records: list[dict]):
    ingested_at = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute("DELETE FROM bronze.country_raw")
        for record in records:
            cur.execute(
                """
                INSERT INTO bronze.country_raw (country_code, raw_record, ingested_at)
                VALUES (%s, %s, %s)
                """,
                (record["id"], json.dumps(record), ingested_at),
            )
    conn.commit()

def land_indicator_metadata(conn, records: list[dict]):
    ingested_at = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute("DELETE FROM bronze.indicator_raw")
        for record in records:
            cur.execute(
                "INSERT INTO bronze.indicator_raw (indicator_code, raw_record, ingested_at) VALUES (%s, %s, %s)",
                (record["id"], json.dumps(record), ingested_at)
            )
    conn.commit()

def land_indicator(conn, indicator_code: str, records: list[dict]):
    ingested_at = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute("DELETE FROM bronze.wdi_raw WHERE indicator_code = %s", (indicator_code,))
        for record in records:
            cur.execute(
                """
                INSERT INTO bronze.wdi_raw (indicator_code, raw_record, ingested_at)
                VALUES (%s, %s, %s)
                """,
                (indicator_code, json.dumps(record), ingested_at),
            )
    conn.commit()


# Orchestration

def run():
    conn = psycopg2.connect(**DB_CONFIG)
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