.PHONY: setup run ingest dbt-build export clean

# Variáveis para evitar depender de ativação manual de venv.
VENV ?= .venv
PY   ?= $(VENV)/bin/python
PIP  ?= $(VENV)/bin/pip
DBT  ?= $(VENV)/bin/dbt

# Instala dependências em ambiente virtual local.
setup:
	python -m venv $(VENV)
	$(PIP) install -r requirements.txt

# Ingestão: CSV -> Parquet (Bronze)
ingest:
	$(PY) ingestion/ingest.py

# Transformações + testes: dbt (Silver/Gold)
dbt-build:
	$(DBT) build --profiles-dir . --project-dir dbt

# Exporta marts (Gold) para CSV
export:
	$(PY) scripts/export_marts.py

# Executa pipeline completo
run: ingest dbt-build export

# Limpa artefatos locais
clean:
	rm -rf data output dbt/target dbt/logs $(VENV)
