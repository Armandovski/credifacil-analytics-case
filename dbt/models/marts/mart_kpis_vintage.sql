-- Mart de KPIs de Vintage (Gold)
-- Grão: coorte_origem_mes + idade_meses + kpi_nome
-- KPI: vintage_default_90 = % de contratos que já atingiram default 90+ até a idade M

with parametros as (
  select 12 as max_idade_meses
),

idades as (
  select range as idade_meses
  from range(1, (select max_idade_meses from parametros) + 1)
),

loans as (
  select
    loan_id,
    data_concessao,
    date_trunc('month', data_concessao) as coorte_origem_mes
  from {{ ref('mart_credit_loans') }}
  where data_concessao is not null
),

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
parcelas_com_default_90 as (
  select
    loan_id,
    case
      -- Parcela paga com 90+ dias de atraso
      when data_pagamento is not null and dpd_efetivo >= 90
        then cast(data_vencimento + interval '90 day' as date)

      -- Parcela ainda não paga e já tem 90+ dias em aberto na data de referência
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

vintage as (
  select
    d.coorte_origem_mes,
    i.idade_meses,
    'vintage_default_90' as kpi_nome,

    cast(
      sum(
        case
          -- elegível e já entrou em default 90+ até a idade M
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
            -- elegível (já atingiu a idade M)
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

select * from vintage