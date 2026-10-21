"""Bronze landing for WDI: writes raw records to Postgres."""

from datetime import datetime, timezone
import json

# Bronze data landing
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