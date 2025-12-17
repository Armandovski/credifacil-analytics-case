with raw as (
  select *
  from {{ ext_parquet('bronze', 'loans_raw') }}
),

normalizado as (
  select
    try_cast(cast(ingestion_date as varchar) as date) as data_ingestao,

    trim(cast(loan_id as varchar)) as loan_id,
    trim(cast(customer_id as varchar)) as customer_id,

    trim(cast(data_concessao as varchar)) as data_concessao_txt,
    trim(cast(valor_contratado as varchar)) as valor_contratado_txt,
    trim(cast(prazo_meses as varchar)) as prazo_meses_txt,
    trim(cast(taxa_juros_anual as varchar)) as taxa_juros_anual_txt,

    lower(trim(cast(canal as varchar))) as canal,
    lower(trim(cast(status as varchar))) as status_reportado
  from raw
),

deduplicado as (
  -- Mantém somente a linha mais recente por loan_id
  select *
  from (
    select
      *,
      row_number() over (
        partition by loan_id
        order by data_ingestao desc nulls last
      ) as rn
    from normalizado
  )
  where rn = 1
),

tipado as (
  select
    data_ingestao,
    loan_id,
    customer_id,

    try_cast(data_concessao_txt as date) as data_concessao,
    try_cast(valor_contratado_txt as double) as valor_contratado,
    try_cast(prazo_meses_txt as integer) as prazo_meses,
    try_cast(taxa_juros_anual_txt as double) as taxa_juros_anual,

    canal,
    status_reportado
  from deduplicado
),

regras as (
  select
    *,

    -- Flags de qualidade
    case when data_concessao is null then true else false end as dq_data_concessao_nula,
    case when valor_contratado is null then true else false end as dq_valor_contratado_nulo,
    case when valor_contratado <= 0 then true else false end as dq_valor_contratado_nao_positivo,

    case when prazo_meses is null then true else false end as dq_prazo_nulo,
    case when prazo_meses <= 0 then true else false end as dq_prazo_nao_positivo,

    case when taxa_juros_anual is null then true else false end as dq_taxa_nula,
    case when taxa_juros_anual < 0 then true else false end as dq_taxa_negativa,

    case
      when status_reportado in ('ativo','liquidado','inadimplente','cancelado') then false
      else true
    end as dq_status_invalido,

    case
      when taxa_juros_anual is null then null
      else (taxa_juros_anual / 100.0)
    end as taxa_anual_decimal
  from tipado
),

com_pmt as (
  select
    *,

    -- PMT teórica (proxy de valor de parcela mensal)
    case
      when taxa_anual_decimal is null or prazo_meses is null or valor_contratado is null then null
      when prazo_meses <= 0 or valor_contratado <= 0 then null
      when taxa_anual_decimal = 0 then valor_contratado / prazo_meses
      else
        (
          valor_contratado *
          (pow(1 + taxa_anual_decimal, 1.0/12.0) - 1)
        ) /
        (
          1 - pow(1 + (pow(1 + taxa_anual_decimal, 1.0/12.0) - 1), -prazo_meses)
        )
    end as pmt_teorica
  from regras
)

select * from com_pmt
