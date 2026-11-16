"""This module lands raw Comtrade records and reference files in the Postgres raw schema."""

from datetime import datetime, timezone
import json

from psycopg2.extras import execute_values

# Which query a record came from. Trade between two African countries appears in both.
REPORTER_SIDE = "reporter"   # the 54 countries' own reports, against every partner
PARTNER_SIDE = "partner"     # every country's reports about the 54

# A refreshed slice must keep at least this share of the rows already in the raw table.
# Comtrade has no fixed grid to check against (who reports changes by year), but a
# year's record count normally only grows as late reports arrive, so a sharp drop
# means the answer is incomplete, not that trade disappeared.
MIN_RETAINED_RATIO = 0.9


class IncompleteLoadError(Exception):
    """The fetched slice looks incomplete, so it must not replace what's in the raw table."""


def check_slice_size(label: str, new_count: int, previous_count: int) -> None:
    """Raises IncompleteLoadError when a refreshed slice would shrink what is already in the raw table.

    The check only refuses when there is something to protect, so a first load of zero rows is allowed.
    """
    if previous_count and new_count == 0:
        raise IncompleteLoadError(
            f"{label}: API returned 0 records; keeping the {previous_count} rows already in the raw table"
        )
    if previous_count and new_count < previous_count * MIN_RETAINED_RATIO:
        raise IncompleteLoadError(
            f"{label}: API returned {new_count} records, down from {previous_count}; keeping existing rows"
        )


def ensure_raw_tables(conn):
    with conn.cursor() as cur:
        cur.execute("""
            CREATE SCHEMA IF NOT EXISTS raw;
            CREATE TABLE IF NOT EXISTS raw.comtrade_raw (
                query_side TEXT NOT NULL,
                period INTEGER NOT NULL,
                raw_record JSONB NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL
            );
            CREATE INDEX IF NOT EXISTS comtrade_raw_slice_idx ON raw.comtrade_raw (query_side, period);
            CREATE TABLE IF NOT EXISTS raw.comtrade_reporters_raw (
                reporter_code INTEGER NOT NULL,
                raw_record JSONB NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL
            );
            CREATE TABLE IF NOT EXISTS raw.comtrade_partner_areas_raw (
                partner_code INTEGER NOT NULL,
                raw_record JSONB NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL
            );
            CREATE TABLE IF NOT EXISTS raw.comtrade_product_raw (
                query_side TEXT NOT NULL,
                period INTEGER NOT NULL,
                raw_record JSONB NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL  
            );
            CREATE INDEX IF NOT EXISTS comtrade_product_raw_slice_idx ON raw.comtrade_product_raw (query_side, period);
            CREATE TABLE IF NOT EXISTS raw.comtrade_hs_raw (
                cmd_code TEXT NOT NULL,
                raw_record JSONB NOT NULL,
                ingested_at TIMESTAMPTZ NOT NULL  
            );
        """)
    conn.commit()


def _replace_reference(conn, label: str, table: str, code_column: str, code_field: str, records: list[dict]):
    """Replaces a whole reference table in one transaction.

    The table and column names always come from constants in this module, never from input.
    """
    ingested_at = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute(f"SELECT count(*) FROM {table}")
        check_slice_size(label, len(records), cur.fetchone()[0])

        cur.execute(f"DELETE FROM {table}")
        execute_values(
            cur,
            f"INSERT INTO {table} ({code_column}, raw_record, ingested_at) VALUES %s",
            [(r[code_field], json.dumps(r), ingested_at) for r in records],
            page_size=1000,
        )
    conn.commit()


def land_reporters(conn, records: list[dict]):
    _replace_reference(conn, "reporter reference", "raw.comtrade_reporters_raw",
                       "reporter_code", "reporterCode", records)


def land_partner_areas(conn, records: list[dict]):
    _replace_reference(conn, "partner area reference", "raw.comtrade_partner_areas_raw",
                       "partner_code", "PartnerCode", records)


def _replace_slice(conn, table: str, query_side: str, period: int, records: list[dict]):
    """Replaces one (query_side, period) slice of a trade table in a single transaction.

    The table name always comes from a constant in the land_* functions below, never from input.
    """
    ingested_at = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*) FROM {table} WHERE query_side = %s AND period = %s",
            (query_side, period),
        )
        check_slice_size(f"{table} {query_side} {period}",
                         len(records), cur.fetchone()[0])

        cur.execute(
            f"DELETE FROM {table} WHERE query_side = %s AND period = %s", (query_side, period))
        execute_values(
            cur,
            f"INSERT INTO {table} (query_side, period, raw_record, ingested_at) VALUES %s",
            [(query_side, period, json.dumps(r), ingested_at)
             for r in records],
            page_size=1000,
        )
    conn.commit()


def land_trade(conn, query_side: str, period: int, records: list[dict]):
    _replace_slice(conn, "raw.comtrade_raw", query_side, period, records)


def land_product_trade(conn, query_side: str, period: int, records: list[dict]):
    _replace_slice(conn, "raw.comtrade_product_raw",
                   query_side, period, records)


def land_hs_reference(conn, records: list[dict]):
    _replace_reference(conn, "HS reference",
                       "raw.comtrade_hs_raw", "cmd_code", "id", records)
