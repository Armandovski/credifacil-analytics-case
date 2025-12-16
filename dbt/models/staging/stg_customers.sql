with raw as (
  select *
  from {{ ext_parquet('bronze', 'customers_raw') }}
),

tipado as (
  select
    cast(trim(cast(customer_id as varchar)) as varchar) as customer_id,
    trim(cast(nome as varchar)) as nome,

    try_cast(trim(cast(data_nascimento as varchar)) as date) as data_nascimento,
    try_cast(trim(cast(renda_mensal as varchar)) as double) as renda_mensal,
    try_cast(trim(cast(score_interno as varchar)) as double) as score_interno,

    upper(trim(cast(estado as varchar))) as estado,
    try_cast(trim(cast(data_cadastro as varchar)) as date) as data_cadastro
  from raw
)

select
  *,
  -- Flags de qualidade
  case when data_nascimento is null then true else false end as dq_data_nascimento_nula,
  case when renda_mensal is null then true else false end as dq_renda_nula,
  case when renda_mensal < 0 then true else false end as dq_renda_negativa,
  case when score_interno is null then true else false end as dq_score_nulo,
  case when estado is null or length(estado) <> 2 then true else false end as dq_estado_invalido
from tipado
