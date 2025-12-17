-- Mart de KPIs de Vintage (Gold)
-- Grão: data_referencia + coorte_origem_mes + idade_meses + kpi_nome
-- KPI: vintage_default_90 = % de contratos que já atingiram default 90+ até a idade M
{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['data_referencia', 'coorte_origem_mes', 'idade_meses', 'kpi_nome']
) }}

with parametros as (
  -- Quantos "meses de idade" queremos calcular no vintage
  select 12 as max_idade_meses
),

-- Data de referência do snapshot (as-of). Vem de --vars no dbt.
data_ref as (
  select {{ data_referencia() }} as data_referencia
),

-- Gera a lista de idades (1..N)
idades as (
  select range as idade_meses
  from range(1, (select max_idade_meses from parametros) + 1)
),

-- Base de empréstimos (coorte por mês de concessão)
loans as (
  select
    loan_id,
    data_concessao,
    date_trunc('month', data_concessao) as coorte_origem_mes
  from {{ ref('mart_credit_loans') }}
  where data_concessao is not null
),

-- Base de parcelas (observadas)
parcelas as (
  select
    loan_id,
    data_vencimento,
    data_pagamento,
    dpd_efetivo
  from {{ ref('mart_credit_installments') }}
  where data_vencimento is not null
),

-- Momento em que cada parcela "cruza" 90 dias de atraso
-- Observação:
-- - Se a parcela foi paga com 90+ dias de atraso, considera-se que ela cruzou 90d em (data_vencimento + 90 dias)
-- - Se não foi paga e já tem >= 90 dias até a data de referência, considera-se que cruzou 90d em (data_vencimento + 90 dias)
parcelas_com_default_90 as (
  select
    loan_id,
    case
      when data_pagamento is not null and dpd_efetivo >= 90
        then cast(data_vencimento + interval '90 day' as date)

      when data_pagamento is null
           and date_diff('day', data_vencimento, {{ data_referencia() }}) >= 90
        then cast(data_vencimento + interval '90 day' as date)

      else null
    end as data_default_90_parcela
  from parcelas
),

-- Primeira vez que o empréstimo entrou em default 90+
default_por_loan as (
  select
    l.loan_id,
    l.data_concessao,
    l.coorte_origem_mes,
    min(p.data_default_90_parcela) as data_default_90
  from loans l
  left join parcelas_com_default_90 p
    on l.loan_id = p.loan_id
  group by 1,2,3
),

-- Calcula o vintage por coorte e idade
vintage as (
  select
    d.coorte_origem_mes,
    i.idade_meses,
    'vintage_default_90' as kpi_nome,

    cast(
      sum(
        case
          -- Elegível na idade M e já entrou em default 90+ até a idade M
          when cast(d.data_concessao + (i.idade_meses * interval '1 month') as date) <= {{ data_referencia() }}
               and d.data_default_90 is not null
               and d.data_default_90 <= cast(d.data_concessao + (i.idade_meses * interval '1 month') as date)
            then 1 else 0
        end
      ) * 1.0
      /
      nullif(
        sum(
          case
            -- Elegível (já atingiu a idade M)
            when cast(d.data_concessao + (i.idade_meses * interval '1 month') as date) <= {{ data_referencia() }}
              then 1 else 0
          end
        ),
        0
      )
      as double
    ) as kpi_valor,

    'Percentual de contratos que já atingiram default (90+ DPD) até a idade M, por coorte de originação.' as kpi_descricao

  from default_por_loan d
  cross join idades i
  group by 1,2,3,5
)

-- Inclui a data do snapshot como coluna (necessário para histórico incremental)
select
  dr.data_referencia,
  v.coorte_origem_mes,
  v.idade_meses,
  v.kpi_nome,
  v.kpi_valor,
  v.kpi_descricao
from vintage v
cross join data_ref dr
