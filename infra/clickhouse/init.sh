#!/bin/bash
set -e

clickhouse-client --query "CREATE DATABASE IF NOT EXISTS gold"
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS silver"

clickhouse-client --query "
CREATE TABLE IF NOT EXISTS silver.stg_countries
(
    country_code String,
    country_name String,
    iso2_code String,
    region_code String,
    region_name String,
    income_level_code String,
    income_level_name String,
    lending_type String,
    capital_city Nullable(String),
    longitude Nullable(Float64),
    latitude Nullable(Float64),
    ingested_at DateTime64(6)
)
ENGINE = PostgreSQL('${DB_HOST}:${DB_PORT}', '${DB_NAME}', 'stg_countries', '${DB_USER}', '${DB_PASSWORD}', 'silver')
"

clickhouse-client --query "
CREATE TABLE IF NOT EXISTS silver.stg_wdi_indicators
(
    country_code String,
    indicator_code String,
    indicator_name String,
    year Int32,
    value Nullable(Float64),
    ingested_at DateTime64(6)
)
ENGINE = PostgreSQL('${DB_HOST}:${DB_PORT}', '${DB_NAME}', 'stg_wdi_indicators', '${DB_USER}', '${DB_PASSWORD}', 'silver')
"

clickhouse-client --query "
CREATE TABLE IF NOT EXISTS silver.dim_country_snapshot
(
    country_code String,
    country_name String,
    iso2_code String,
    region_code String,
    region_name String,
    income_level_code String,
    income_level_name String,
    lending_type String,
    capital_city Nullable(String),
    longitude Nullable(Float64),
    latitude Nullable(Float64),
    dbt_scd_id String,
    dbt_updated_at DateTime64(6),
    dbt_valid_from DateTime64(6),
    dbt_valid_to Nullable(DateTime64(6))
)
ENGINE = PostgreSQL('${DB_HOST}:${DB_PORT}', '${DB_NAME}', 'dim_country_snapshot', '${DB_USER}', '${DB_PASSWORD}', 'silver')
"

clickhouse-client --query "
CREATE TABLE IF NOT EXISTS silver.income_classification_history
(
    country_code String,
    income_level_name String,
    valid_from Date,
    valid_to Date
)
ENGINE = PostgreSQL('${DB_HOST}:${DB_PORT}', '${DB_NAME}', 'income_classification_history', '${DB_USER}', '${DB_PASSWORD}', 'silver')
"

clickhouse-client --query "
CREATE TABLE IF NOT EXISTS silver.stg_indicators
(
    indicator_code String,
    indicator_name Nullable(String),
    unit Nullable(String),
    category Nullable(String)
)
ENGINE = PostgreSQL('${DB_HOST}:${DB_PORT}', '${DB_NAME}', 'stg_indicators', '${DB_USER}', '${DB_PASSWORD}', 'silver')
"

echo "ClickHouse init complete."