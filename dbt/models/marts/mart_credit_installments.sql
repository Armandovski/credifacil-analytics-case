-- Mart (Gold) no grão de parcela (loan_id + numero_parcela + data_vencimento).
-- Foco: cobrança, aging, priorização e análises de atraso.

with parcelas as (
  select
    loan_id,
    customer_id,
    numero_parcela,
    data_vencimento,
    data_pagamento,
    valor_devido,
    valor_pago,
    valor_em_aberto,
    status_parcela,
    dpd_efetivo,
    data_corte,
    dq_parcela_sem_registro_pagamento
  from {{ ref('int_installments') }}
),

loans as (
  select
    loan_id,
    data_concessao,
    valor_contratado,
    prazo_meses,
    taxa_juros_anual,
    canal,
    status_reportado,
    pmt_teorica
  from {{ ref('stg_loans') }}
),

clientes as (
  select
    customer_id,
    nome,
    data_nascimento,
    renda_mensal,
    score_interno,
    estado,
    data_cadastro
  from {{ ref('stg_customers') }}
)

select
  p.loan_id,
  p.customer_id,

  p.numero_parcela,
  p.data_vencimento,
  p.data_pagamento,

  p.valor_devido,
  p.valor_pago,
  p.valor_em_aberto,

  p.status_parcela,
  p.dpd_efetivo,
  p.data_corte,

  l.data_concessao,
  l.valor_contratado,
  l.prazo_meses,
  l.taxa_juros_anual,
  l.canal,
  l.status_reportado,
  l.pmt_teorica,

  c.nome,
  c.data_nascimento,
  c.renda_mensal,
  c.score_interno,
  c.estado,
  c.data_cadastro,

  p.dq_parcela_sem_registro_pagamento
from parcelas p
left join loans l
  on l.loan_id = p.loan_id
left join clientes c
  on c.customer_id = p.customer_id
