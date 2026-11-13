"""Bronze landing for WDI: writes raw records to Postgres."""

from datetime import datetime, timezone
import json

from psycopg2.extras import execute_values

from config import AFRICA_ISO3, START_YEAR

from .indicators import INDICATORS


class IncompleteLoadError(Exception):
    """The fetched data is not a complete set, so it must not replace what's in bronze."""


def check_ids_complete(label: str, ids: list[str], expected: set[str]) -> None:
    """Metadata must cover exactly the expected codes, each once."""
    found = set(ids)
    missing, extra = expected - found, found - expected
    duplicates = len(ids) - len(found)
    if missing or extra or duplicates:
        raise IncompleteLoadError(
            f"{label}: missing {sorted(missing)}, unexpected {sorted(extra)}, "
            f"{duplicates} duplicates; keeping existing rows"
        )


def check_indicator_grid(
    label: str,
    keys: list[tuple[str, int]],
    countries: set[str],
    start_year: int,
    previous_max_year: int | None,
) -> None:
    """An indicator pull must be a full country x year grid.

    WDI returns a row for every country-year in range, with a null value where
    there is no data, so a complete pull has every country for every year from
    start_year to its latest year, each exactly once. The latest year is not
    fixed, but it must never fall below what bronze already holds.
    """
    def fail(reason: str):
        raise IncompleteLoadError(f"{label}: {reason}; keeping existing rows")

    if not keys:
        fail("API returned 0 records")
    if len(set(keys)) != len(keys):
        fail(f"{len(keys) - len(set(keys))} duplicate country-year rows")

    found_countries = {country for country, _ in keys}
    if found_countries != countries:
        fail(f"missing countries {sorted(countries - found_countries)}, "
             f"unexpected {sorted(found_countries - countries)}")

    years = sorted({year for _, year in keys})
    if years != list(range(start_year, years[-1] + 1)):
        fail(
            f"years are not a continuous run from {start_year} (got {years[0]}..{years[-1]})")
    if len(keys) != len(countries) * len(years):
        fail(
            f"{len(keys)} rows, expected {len(countries)} countries x {len(years)} years")
    if previous_max_year is not None and years[-1] < previous_max_year:
        fail(f"latest year fell from {previous_max_year} to {years[-1]}")


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
        check_ids_complete("country metadata", [
                           r["id"] for r in records], AFRICA_ISO3)

        cur.execute("DELETE FROM bronze.country_raw")
        execute_values(
            cur,
            "INSERT INTO bronze.country_raw (country_code, raw_record, ingested_at) VALUES %s",
            [(r["id"], json.dumps(r), ingested_at) for r in records],
            page_size=1000,
        )
    conn.commit()


def land_indicator_metadata(conn, records: list[dict]):
    ingested_at = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        check_ids_complete("indicator metadata", [
                           r["id"] for r in records], set(INDICATORS))

        cur.execute("DELETE FROM bronze.indicator_raw")
        execute_values(
            cur,
            "INSERT INTO bronze.indicator_raw (indicator_code, raw_record, ingested_at) VALUES %s",
            [(r["id"], json.dumps(r), ingested_at) for r in records],
            page_size=1000,
        )
    conn.commit()


def land_indicator(conn, indicator_code: str, records: list[dict]):
    ingested_at = datetime.now(timezone.utc)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT max((raw_record ->> 'date')::int) FROM bronze.wdi_raw WHERE indicator_code = %s",
            (indicator_code,),
        )
        previous_max_year = cur.fetchone()[0]
        keys = [(r["countryiso3code"], int(r["date"])) for r in records]
        check_indicator_grid(indicator_code, keys,
                             AFRICA_ISO3, START_YEAR, previous_max_year)

        cur.execute(
            "DELETE FROM bronze.wdi_raw WHERE indicator_code = %s", (indicator_code,))
        execute_values(
            cur,
            "INSERT INTO bronze.wdi_raw (indicator_code, raw_record, ingested_at) VALUES %s",
            [(indicator_code, json.dumps(r), ingested_at) for r in records],
            page_size=1000,
        )
    conn.commit()
