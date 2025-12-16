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
flowchart TD
  %% STAGING
  subgraph STAGING["Staging (Silver inicial)"]
    STG_CUSTOMERS["stg_customers\n(grão: customer_id)\n- try_cast + trim\n- dq_*"]
    STG_LOANS["stg_loans\n(grão: loan_id)\n- pmt_teorica\n- status_reportado\n- dq_*"]
    STG_PAYMENTS["stg_payments\n(grão: payment_id)\n- tipagem\n- dq_* (datas/valores)"]
  end

  %% INTERMEDIATE
  subgraph INTERMEDIATE["Intermediate (Silver)"]
    INT_SCHEDULE["int_installment_schedule\n(grão: loan_id + parcela)\n- range(1..prazo)\n- data_vencimento (proxy)"]
    INT_INSTALLMENTS["int_installments\n(grão: loan_id + parcela)\n- join schedule x payments\n- valor_em_aberto\n- status_parcela\n- dpd_efetivo"]
    INT_LOAN_PERF["int_loan_performance\n(grão: loan_id)\n- saldo_em_aberto\n- dpd_atual\n- status_derivado"]
  end

  %% MARTS
  subgraph MARTS["Marts (Gold)"]
    MART_INSTALLMENTS["mart_credit_installments\n(grão: parcela)\n- visão cobrança/aging"]
    MART_LOANS["mart_credit_loans\n(grão: contrato)\n- visão risco/produto"]
    MART_KPIS["mart_kpis_daily\n(grão: data + kpi)\n- inadimplência, PAR30/90,\n  yield, múltiplos contratos"]
  end

  %% Edges
  STG_LOANS --> INT_SCHEDULE
  STG_PAYMENTS --> INT_INSTALLMENTS
  INT_SCHEDULE --> INT_INSTALLMENTS

  STG_LOANS --> INT_LOAN_PERF
  INT_INSTALLMENTS --> INT_LOAN_PERF

  INT_INSTALLMENTS --> MART_INSTALLMENTS
  STG_CUSTOMERS --> MART_LOANS
  STG_LOANS --> MART_LOANS
  INT_LOAN_PERF --> MART_LOANS

  MART_INSTALLMENTS --> MART_KPIS
  MART_LOANS --> MART_KPIS

  %% Styling
  classDef silverLayer fill:#d9d9d9,stroke:#808080,stroke-width:2px;
  classDef goldLayer fill:#ffd700,stroke:#bfa200,stroke-width:2px;

  class STAGING,INTERMEDIATE silverLayer;
  class MARTS goldLayer;