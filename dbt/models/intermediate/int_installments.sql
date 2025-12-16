-- Consolidação de parcelas (grade esperada + pagamentos).
-- Grão: loan_id + numero_parcela + data_vencimento

with schedule as (
  select
    loan_id,
    customer_id,
    data_concessao,
    prazo_meses,
    status_reportado,
    pmt_teorica,
    numero_parcela,
    data_vencimento
  from {{ ref('int_installment_schedule') }}
),

payments as (
  select
    loan_id,
    data_vencimento,
    data_pagamento,
    valor_parcela,
    valor_pago,
    atraso_dias
  from {{ ref('stg_payments') }}
),

juncao as (
  select
    s.loan_id,
    s.customer_id,
    s.data_concessao,
    s.prazo_meses,
    s.status_reportado,
    s.pmt_teorica,
    s.numero_parcela,
    s.data_vencimento,

    p.data_pagamento,
    p.valor_parcela as valor_parcela_informada,
    p.valor_pago as valor_pago_informado,
    p.atraso_dias as atraso_dias_informado,

    case when p.loan_id is null then true else false end as dq_parcela_sem_registro_pagamento
  from schedule s
  left join payments p
    on p.loan_id = s.loan_id
   and p.data_vencimento = s.data_vencimento
),

valores as (
  select
    *,
    -- Se não veio valor_parcela em payments, usamos a PMT teórica como aproximação
    coalesce(valor_parcela_informada, pmt_teorica) as valor_devido,
    coalesce(valor_pago_informado, 0.0) as valor_pago,
    greatest(coalesce(valor_parcela_informada, pmt_teorica) - coalesce(valor_pago_informado, 0.0), 0.0) as valor_em_aberto,

    case
      when coalesce(valor_pago_informado, 0.0) = 0 and coalesce(valor_parcela_informada, pmt_teorica) > 0 then 'nao_pago'
      when coalesce(valor_pago_informado, 0.0) >= coalesce(valor_parcela_informada, pmt_teorica) and coalesce(valor_parcela_informada, pmt_teorica) > 0 then 'pago_integral'
      when coalesce(valor_pago_informado, 0.0) > 0 and coalesce(valor_pago_informado, 0.0) < coalesce(valor_parcela_informada, pmt_teorica) then 'pago_parcial'
      else 'indefinido'
    end as status_parcela
  from juncao
),

data_corte as (
  -- Evitar o nome "asof" (palavra reservada no DuckDB).
  select
    coalesce(try_cast('{{ var("data_corte", "") }}' as date), current_date) as data_corte
),

dpd as (
  select
    v.*,
    dc.data_corte,

    case
      -- Se existe saldo em aberto e a parcela venceu, atraso é em relação à data de corte
      when v.valor_em_aberto > 0 and v.data_vencimento < dc.data_corte
        then date_diff('day', v.data_vencimento, dc.data_corte)

      -- Se a parcela foi paga integralmente e tem data_pagamento, atraso é real
      when v.status_parcela = 'pago_integral' and v.data_pagamento is not null
        then date_diff('day', v.data_vencimento, v.data_pagamento)

      else 0
    end as dpd_efetivo
  from valores v
  cross join data_corte dc
)

select * from dpd
