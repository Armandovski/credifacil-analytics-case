FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV INPUT_DIR=/app/input
ENV DATA_DIR=/app/data
ENV BRONZE_DIR=/app/data/bronze
ENV DUCKDB_PATH=/app/data/warehouse.duckdb
ENV OUTPUT_DIR=/app/output
ENV AS_OF_DATE=2025-12-04
ENV DBT_PROFILES_DIR=/app

RUN chmod +x scripts/entrypoint.sh

ENTRYPOINT ["bash", "scripts/entrypoint.sh"]
