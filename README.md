# Credifácil — Case de Analytics Engineering (Medallion + dbt + DuckDB)

Este repositório entrega um pipeline de dados **executável** para um case fictício de crédito pessoal (Credifácil),
com arquitetura **Medallion** (Bronze/Silver/Gold), transformações em **dbt/SQL** e *warehouse local* em **DuckDB**.

A intenção é simular um cenário realista (GCP hoje, Databricks amanhã) sem depender de infraestrutura externa.

## Visão geral

- **Bronze (raw):** ingestão dos CSVs e gravação em **Parquet** (simulando data lake).
- **Silver (curated):** padronização de tipos, limpeza, normalização e flags de qualidade.
- **Gold (marts):** tabelas analíticas prontas para consumo (Risco, Cobrança, Produto).

Principais marts:
- `mart_credit_installments` (grão: parcela)
- `mart_credit_loans` (grão: contrato)
- `mart_kpis_daily` (grão: dia)

## Como rodar (local)

1) Coloque os arquivos em `./input/`:
- `customers.csv`
- `loans.csv`
- `payments.csv`

2) Rode:
```bash
cp .env.example .env
make setup
make run
```

Saídas:
- `output/mart_credit_installments.csv`
- `output/mart_credit_loans.csv`
- `output/mart_kpis_daily.csv`

## Como rodar (Docker)

Build + run:
```bash
docker build -t credifacil-ae .
docker run --rm \
  -v "$(pwd)/input:/app/input" \
  -v "$(pwd)/output:/app/output" \
  credifacil-ae
```

Ou via compose:
```bash
docker compose up --build
```

## Observações de qualidade (exemplo real do dataset)
- A coluna `prazo_meses` pode conter **espaços em branco** em algumas linhas.
  - Na **Bronze**, os dados são lidos como texto.
  - Na **Silver**, aplicamos `trim()` + `try_cast()` para garantir tipagem consistente.

## Estrutura do projeto

- `ingestion/`: ingestão Bronze (Python)
- `dbt/`: modelos SQL (Silver/Gold) + testes
- `scripts/`: entrypoint e exportação de marts
- `data/`: armazenamento local (Parquet + DuckDB)
- `output/`: exportação final em CSV
- `docs/`: decisões e arquitetura (GCP → Databricks)

Veja mais detalhes em `docs/ARQUITETURA.md`.
