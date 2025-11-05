FROM python:3.11-slim-bookworm

ENV PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    git ca-certificates \
 && rm -rf /var/lib/apt/lists/*

ARG DBT_CORE_VERSION=1.10.13
ARG DBT_CLICKHOUSE_VERSION=1.9.6

RUN python -m pip install --upgrade pip \
 && pip install \
    "dbt-core==${DBT_CORE_VERSION}" \
    "dbt-clickhouse==${DBT_CLICKHOUSE_VERSION}"

WORKDIR /app

COPY ./digipoc/ /app

RUN dbt --version

CMD ["dbt", "debug"]