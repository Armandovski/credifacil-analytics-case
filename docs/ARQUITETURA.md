# Arquitetura (GCP hoje → Databricks amanhã)

Este case foi desenhado para ser **portável** entre stacks.

## Princípios
- Arquitetura **Medallion**: Bronze → Silver → Gold
- Formato de lake: **Parquet** (e, no futuro, **Delta**/Iceberg/Hudi)
- Transformações: **SQL/dbt** (pushdown para o engine)
- Python: eda/ingestão/orquestração/validação leve (não como motor principal)

## Implementação local (para o case)
- “Lake” local: `data/bronze/...` em Parquet
- “Warehouse” local: `data/warehouse.duckdb`
- dbt materializa Silver e Gold dentro do DuckDB
- Exporta marts para `output/`

## Como isso mapeia para GCP
- Storage: **GCS**
- Compute/serving: **BigQuery**
- Modelagem/transformação: **dbt (bigquery)**
- Orquestração: **Cloud Composer (Airflow)** ou Cloud Run Jobs + Scheduler

## Como isso evolui para Databricks
- Storage pode continuar em **GCS**
- Compute: **Databricks (Spark SQL)**
- Silver/Gold podem ser materializados em **Delta Lake**
- dbt pode migrar de `dbt-bigquery` para `dbt-databricks` com o mesmo desenho lógico

## Observabilidade e qualidade
Em produção, eu complementaria com:
- Métricas de saúde do dado (% nulos, casts inválidos, atraso de ingestão)
- Logs estruturados
- Testes dbt adicionais (ex.: accepted_values, relacionamentos, expressões)
- Tabela de auditoria (run_id, data_ingestao, contagens por camada)

```mermaid
flowchart LR
  bronze_customers[bronze.customers] --> stg_customers[stg_customers]
  bronze_loans[bronze.loans] --> stg_loans[stg_loans]
  bronze_payments[bronze.payments] --> stg_payments[stg_payments]

  stg_loans --> int_installment_schedule[int_installment_schedule]
  int_installment_schedule --> int_installments[int_installments]
  stg_payments --> int_installments
  stg_customers --> mart_credit_loans[mart_credit_loans]
  stg_loans --> mart_credit_loans
  int_installments --> mart_credit_installments[mart_credit_installments]

  mart_credit_loans --> mart_kpis_daily[mart_kpis_daily]
  mart_credit_installments --> mart_kpis_daily

  mart_credit_loans --> mart_kpis_vintage[mart_kpis_vintage]