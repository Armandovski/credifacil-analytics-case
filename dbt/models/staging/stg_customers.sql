with raw as (
  select *
  from {{ ext_parquet('bronze', 'customers_raw') }}
),

normalizado as (
  select
    -- Partição do lake (hive partitioning)
    try_cast(cast(ingestion_date as varchar) as date) as data_ingestao,

    -- Campos originais (normalizados como texto antes de tipar)
    trim(cast(customer_id as varchar)) as customer_id,
    trim(cast(nome as varchar)) as nome,
    trim(cast(data_nascimento as varchar)) as data_nascimento_txt,
    trim(cast(renda_mensal as varchar)) as renda_mensal_txt,
    trim(cast(score_interno as varchar)) as score_interno_txt,
    trim(cast(estado as varchar)) as estado_txt,
    trim(cast(data_cadastro as varchar)) as data_cadastro_txt
  from raw
),

deduplicado as (
  -- Mantém somente a linha mais recente por customer_id (com base em data_ingestao)
  select *
  from (
    select
      *,
      row_number() over (
        partition by customer_id
        order by data_ingestao desc nulls last
      ) as rn
    from normalizado
  )
  where rn = 1
),

tipado as (
  select
    data_ingestao,

    cast(customer_id as varchar) as customer_id,
    nome,

    try_cast(data_nascimento_txt as date) as data_nascimento,
    try_cast(renda_mensal_txt as double) as renda_mensal,
    try_cast(score_interno_txt as double) as score_interno,

    upper(estado_txt) as estado,
    try_cast(data_cadastro_txt as date) as data_cadastro
  from deduplicado
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
