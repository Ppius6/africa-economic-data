FROM ghcr.io/astral-sh/uv:python3.14-bookworm-slim

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY dagster_project/pyproject.toml dagster_project/pyproject.toml
RUN uv sync --locked --no-install-workspace

COPY . .
RUN uv sync --locked

WORKDIR /app/dbt_project
RUN dbt deps
RUN POSTGRES_USER=build POSTGRES_PASSWORD=build CLICKHOUSE_USER=build CLICKHOUSE_PASSWORD=build \
    dbt parse --profiles-dir .

WORKDIR /app
