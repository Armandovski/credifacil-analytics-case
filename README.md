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
- `mart_kpis_vintage` (grão: coorte_origem_mes + idade_meses + kpi_nome)

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
- `output/mart_kpis_vintage.csv`

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

## Métricas e Indicadores de Crédito (KPIs)

Esta entrega inclui uma camada de *marts* (Gold) com KPIs de risco e performance para apoiar decisões dos times de **Risco**, **Cobrança** e **Produto**. Os KPIs são calculados como **snapshot** na `data_referencia` (parametrizada, tipicamente alinhada à `ingestion_date`), permitindo reprocessamento e backfill para datas específicas.

### Onde os KPIs são calculados

- **`mart_kpis_daily`**: KPIs agregados do portfólio (snapshot diário).
  - **Grão**: `data_referencia + kpi_nome`
- **`mart_kpis_vintage`**: KPIs por safra (coorte) e idade do contrato.
  - **Grão**: `data_referencia + coorte_origem_mes + idade_meses + kpi_nome`

### Tabela de KPIs

| Métrica (kpi_nome) | Descrição (o que mede) |
|---|---|
| **taxa_inadimplencia_contratos** | Percentual de contratos em inadimplência (usando **status derivado** e/ou critério de **DPD ≥ 90**). |
| **par30_valor_em_aberto** | Soma do **valor em aberto** de parcelas com **DPD ≥ 30** (proxy de **PAR30**). |
| **par90_valor_em_aberto** | Soma do **valor em aberto** de parcelas com **DPD ≥ 90** (proxy de **PAR90**). |
| **pct_clientes_multiplos_contratos** | Percentual de clientes com **mais de um contrato** (indicador de concentração de risco em recorrentes). |
| **yield_efetivo_aprox** | Razão entre **valor pago** e **valor devido** nas parcelas observadas (proxy de yield/realização financeira). |
| **pct_parcelas_pagas_em_dia** | Percentual de parcelas vencidas que foram **pagas em dia** (indicador de disciplina de pagamento e/ou efetividade de cobrança). |
| **vintage_default_90** | Taxa de default (**90+ DPD**) por **coorte de originação** e **idade do contrato (meses)**, para acompanhar deterioração por safra ao longo do tempo. |

### Observações e premissas importantes

- **DPD / default 90+**: o conceito de inadimplência severa é tratado como **DPD ≥ 90** (e/ou status derivado equivalente), coerente com a métrica sugerida no enunciado.
- **PAR30/PAR90**: como este é um case simplificado (sem saldo contábil de principal/juros por contrato), o PAR foi aproximado via **`valor_em_aberto`** das parcelas com atraso acima do threshold.
- **Yield efetivo**: usado como proxy simples para “realização financeira” a partir de **pagamentos observados**, não como taxa interna de retorno (TIR).
- **Vintage**: o `vintage_default_90` mede a proporção de contratos que **já** atingiram default 90+ **até** a idade M (em meses), por coorte de originação, respeitando a `data_referencia` do snapshot.
