with raw as (
  select *
  from {{ ext_parquet('bronze', 'payments_raw') }}
),

normalizado as (
  select
    try_cast(cast(ingestion_date as varchar) as date) as data_ingestao,

    trim(cast(payment_id as varchar)) as payment_id,
    trim(cast(loan_id as varchar)) as loan_id,

    trim(cast(data_vencimento as varchar)) as data_vencimento_txt,
    trim(cast(data_pagamento as varchar)) as data_pagamento_txt,

    trim(cast(valor_parcela as varchar)) as valor_parcela_txt,
    trim(cast(valor_pago as varchar)) as valor_pago_txt,
    trim(cast(atraso_dias as varchar)) as atraso_dias_txt
  from raw
),

deduplicado as (
  select *
  from (
    select
      *,
      row_number() over (
        partition by payment_id
        order by data_ingestao desc nulls last
      ) as rn
    from normalizado
    where data_ingestao <= {{ data_referencia() }}
  )
  where rn = 1
),


tipado as (
  select
    data_ingestao,

    payment_id,
    loan_id,

    try_cast(data_vencimento_txt as date) as data_vencimento,
    try_cast(data_pagamento_txt as date) as data_pagamento,

    try_cast(valor_parcela_txt as double) as valor_parcela,
    try_cast(valor_pago_txt as double) as valor_pago,

    try_cast(atraso_dias_txt as integer) as atraso_dias
  from deduplicado
)

select
  *,
  -- Flags de qualidade
  case when data_vencimento is null then true else false end as dq_data_vencimento_nula,
  case when valor_parcela is null then true else false end as dq_valor_parcela_nulo,
  case when valor_parcela < 0 then true else false end as dq_valor_parcela_negativo,
  case when valor_pago is null then true else false end as dq_valor_pago_nulo,
  case when valor_pago < 0 then true else false end as dq_valor_pago_negativo,

  -- Datas invertidas (quando há pagamento)
  case
    when data_pagamento is not null and data_vencimento is not null and data_pagamento < data_vencimento
      then true
    else false
  end as dq_data_pagamento_antes_vencimento
from tipado