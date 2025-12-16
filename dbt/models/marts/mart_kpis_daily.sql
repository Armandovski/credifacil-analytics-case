-- Mart de KPIs diários (Gold)
-- Grão: data_referencia + kpi_nome

with base_loans as (
  select *
  from {{ ref('mart_credit_loans') }}
),

base_parcelas as (
  select *
  from {{ ref('mart_credit_installments') }}
),

data_ref as (
  select current_date as data_referencia
),

kpis as (

  -- KPI 1: Taxa de inadimplência (por contratos)
  select
    any_value(dr.data_referencia) as data_referencia,
    'taxa_inadimplencia_contratos' as kpi_nome,
    cast(
      sum(case when status_derivado = 'inadimplente' then 1 else 0 end) * 1.0
      / nullif(count(*), 0)
      as double
    ) as kpi_valor,
    'Percentual de contratos em inadimplência (status derivado >= 90 dpd).' as kpi_descricao
  from base_loans bl
  cross join data_ref dr

  union all

  -- KPI 2: PAR30 (Portfolio at Risk 30+) aproximado pelo valor_em_aberto
  select
    any_value(dr.data_referencia) as data_referencia,
    'par30_valor_em_aberto' as kpi_nome,
    cast(sum(case when dpd_efetivo >= 30 then valor_em_aberto else 0 end) as double) as kpi_valor,
    'Soma do valor em aberto em parcelas com dpd >= 30 (aproximação de PAR30).' as kpi_descricao
  from base_parcelas bp
  cross join data_ref dr

  union all

  -- KPI 3: PAR90 (Portfolio at Risk 90+)
  select
    any_value(dr.data_referencia) as data_referencia,
    'par90_valor_em_aberto' as kpi_nome,
    cast(sum(case when dpd_efetivo >= 90 then valor_em_aberto else 0 end) as double) as kpi_valor,
    'Soma do valor em aberto em parcelas com dpd >= 90 (aproximação de PAR90).' as kpi_descricao
  from base_parcelas bp
  cross join data_ref dr

  union all

  -- KPI 4: % clientes com múltiplos contratos
  select
    any_value(dr.data_referencia) as data_referencia,
    'pct_clientes_multiplos_contratos' as kpi_nome,
    cast(
      sum(case when qtd_contratos_cliente > 1 then 1 else 0 end) * 1.0
      / nullif(count(*), 0)
      as double
    ) as kpi_valor,
    'Percentual de clientes com mais de um contrato (concentração de risco em recorrentes).' as kpi_descricao
  from (
    select customer_id, count(*) as qtd_contratos_cliente
    from base_loans
    group by 1
  ) t
  cross join data_ref dr

  union all

  -- KPI 5: Yield efetivo aproximado (valor_pago / valor_devido)
  select
    any_value(dr.data_referencia) as data_referencia,
    'yield_efetivo_aprox' as kpi_nome,
    cast(
      sum(valor_pago) / nullif(sum(valor_devido), 0)
      as double
    ) as kpi_valor,
    'Razão entre valor pago e valor devido (proxy de yield realizado) nas parcelas observadas.' as kpi_descricao
  from base_parcelas bp
  cross join data_ref dr
)

select * from kpis