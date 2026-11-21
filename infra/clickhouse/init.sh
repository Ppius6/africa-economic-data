#!/bin/bash
set -e

clickhouse-client --query "CREATE DATABASE IF NOT EXISTS marts"
clickhouse-client --query "CREATE DATABASE IF NOT EXISTS pg_bridge"

clickhouse-client --query "
CREATE TABLE IF NOT EXISTS pg_bridge.stg_countries
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
ENGINE = PostgreSQL('${DB_HOST}:${DB_PORT}', '${DB_NAME}', 'stg_countries', '${DB_USER}', '${DB_PASSWORD}', 'staging')
"

clickhouse-client --query "
CREATE TABLE IF NOT EXISTS pg_bridge.dim_country_snapshot
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
ENGINE = PostgreSQL('${DB_HOST}:${DB_PORT}', '${DB_NAME}', 'dim_country_snapshot', '${DB_USER}', '${DB_PASSWORD}', 'core')
"

clickhouse-client --query "
CREATE TABLE IF NOT EXISTS pg_bridge.income_classification_history
(
    country_code String,
    income_level_name String,
    valid_from Date,
    valid_to Date
)
ENGINE = PostgreSQL('${DB_HOST}:${DB_PORT}', '${DB_NAME}', 'income_classification_history', '${DB_USER}', '${DB_PASSWORD}', 'core')
"

clickhouse-client --query "
CREATE TABLE IF NOT EXISTS pg_bridge.stg_indicators
(
    indicator_code String,
    indicator_name Nullable(String),
    unit Nullable(String),
    category Nullable(String)
)
ENGINE = PostgreSQL('${DB_HOST}:${DB_PORT}', '${DB_NAME}', 'stg_indicators', '${DB_USER}', '${DB_PASSWORD}', 'staging')
"

clickhouse-client --query "
CREATE TABLE IF NOT EXISTS pg_bridge.fct_indicator_value_snapshot
(
    country_code String,
    indicator_code String,
    year Int32,
    value Nullable(Float64),
    dbt_scd_id String,
    dbt_updated_at DateTime64(6),
    dbt_valid_from DateTime64(6),
    dbt_valid_to Nullable(DateTime64(6))
)
ENGINE = PostgreSQL('${DB_HOST}:${DB_PORT}', '${DB_NAME}', 'fct_indicator_value_snapshot', '${DB_USER}', '${DB_PASSWORD}', 'core')
"

clickhouse-client --query "
CREATE TABLE IF NOT EXISTS pg_bridge.stg_comtrade_areas
(
    m49_code Int32,
    area_name String,
    area_type String,
    iso3 Nullable(String),
    is_in_scope Bool,
    valid_from Nullable(Date32),
    valid_to Nullable(Date32),
    ingested_at DateTime64(6)
)
ENGINE = PostgreSQL('${DB_HOST}:${DB_PORT}', '${DB_NAME}', 'stg_comtrade_areas', '${DB_USER}', '${DB_PASSWORD}', 'staging')
"

clickhouse-client --query "
CREATE TABLE IF NOT EXISTS pg_bridge.int_comtrade_bilateral_flows
(
    year Int32,
    exporter_m49 Int32,
    exporter_iso3 Nullable(String),
    exporter_name String,
    exporter_area_type String,
    exporter_in_scope Bool,
    importer_m49 Int32,
    importer_iso3 Nullable(String),
    importer_name String,
    importer_area_type String,
    importer_in_scope Bool,
    exporter_reported_usd Nullable(Float64),
    importer_reported_usd Nullable(Float64),
    importer_value_basis Nullable(String),
    reported_by String,
    is_intra_african Bool,
    exporter_hs_edition Nullable(String),
    importer_hs_edition Nullable(String)
)
ENGINE = PostgreSQL('${DB_HOST}:${DB_PORT}', '${DB_NAME}', 'int_comtrade_bilateral_flows', '${DB_USER}', '${DB_PASSWORD}', 'core')
"

clickhouse-client --query "
CREATE TABLE IF NOT EXISTS pg_bridge.stg_comtrade_hs
(
    hs_code String,
    hs_description String,
    aggr_level Int32,
    parent_hs_code Nullable(String),
    is_leaf Bool,
    ingested_at DateTime64(6)
)
ENGINE = PostgreSQL('${DB_HOST}:${DB_PORT}', '${DB_NAME}', 'stg_comtrade_hs', '${DB_USER}', '${DB_PASSWORD}', 'staging')
"

clickhouse-client --query "
CREATE TABLE IF NOT EXISTS pg_bridge.int_comtrade_product_flows
(
    year Int32,
    exporter_m49 Int32,
    importer_m49 Int32,
    hs_chapter String,
    exporter_reported_usd Nullable(Float64),
    importer_reported_usd Nullable(Float64),
    importer_value_basis Nullable(String),
    reported_by String,
    exporter_in_scope Bool,
    importer_in_scope Bool,
    is_intra_african Bool,
    exporter_hs_edition Nullable(String),
    importer_hs_edition Nullable(String)
)
ENGINE = PostgreSQL('${DB_HOST}:${DB_PORT}', '${DB_NAME}', 'int_comtrade_product_flows', '${DB_USER}', '${DB_PASSWORD}', 'core')
"

echo "ClickHouse init complete."
