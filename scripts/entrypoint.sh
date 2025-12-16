#!/usr/bin/env bash
set -euo pipefail

mkdir -p /app/output

echo "[1/3] Ingestão: CSV -> Parquet (camada Bronze)..."
python /app/ingestion/ingest.py

echo "[2/3] Transformações e testes: dbt build (Silver/Gold no DuckDB)..."
cd /app/dbt
dbt build --profiles-dir /app --project-dir /app/dbt

echo "[3/3] Export: marts (Gold) -> CSV em /app/output..."
python /app/scripts/export_marts.py

echo "Concluído."
ls -lah /app/output || true

