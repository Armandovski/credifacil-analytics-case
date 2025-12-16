with raw as (
  select *
  from {{ ext_parquet('bronze', 'payments_raw') }}
),

tipado as (
  select
    cast(trim(cast(payment_id as varchar)) as varchar) as payment_id,
    cast(trim(cast(loan_id as varchar)) as varchar) as loan_id,

    try_cast(trim(cast(data_vencimento as varchar)) as date) as data_vencimento,
    try_cast(trim(cast(data_pagamento as varchar)) as date) as data_pagamento,

    try_cast(trim(cast(valor_parcela as varchar)) as double) as valor_parcela,
    try_cast(trim(cast(valor_pago as varchar)) as double) as valor_pago,

    try_cast(trim(cast(atraso_dias as varchar)) as integer) as atraso_dias
  from raw
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