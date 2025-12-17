-- Mart de KPIs de Vintage (Gold)
-- Grão: coorte_origem_mes + idade_meses + kpi_nome

with loans as (
  select
    loan_id,
    customer_id,
    data_concessao,
    date_trunc('month', data_concessao) as coorte_origem_mes,
    dpd_atual,
    status_derivado
  from {{ ref('mart_credit_loans') }}
  where data_concessao is not null
),

parametros as (
  -- Ajuste a janela conforme seu apetite (12 ou 18 meses costuma ser suficiente)
  select 12 as max_idade_meses
),

idades as (
  -- Gera idades de 1..max_idade_meses
  select
    range as idade_meses
  from range(1, (select max_idade_meses from parametros) + 1)
),

vintage as (
  select
    l.coorte_origem_mes,
    i.idade_meses,

    'vintage_default_90' as kpi_nome,

    -- Proxy de "observado": contrato já tem pelo menos 'idade_meses' desde a concessão
    cast(
      sum(
        case
          when date_diff('month', l.data_concessao, {{ data_referencia() }}) >= i.idade_meses
               and l.dpd_atual >= 90
            then 1 else 0
        end
      ) * 1.0
      /
      nullif(
        sum(
          case
            when date_diff('month', l.data_concessao, {{ data_referencia() }}) >= i.idade_meses
              then 1 else 0
          end
        ),
        0
      )
      as double
    ) as kpi_valor,

    'Taxa de default (DPD>=90) por coorte de originação e idade do contrato (em meses).' as kpi_descricao

  from loans l
  cross join idades i
  group by 1,2,3,5
)

select * from vintage