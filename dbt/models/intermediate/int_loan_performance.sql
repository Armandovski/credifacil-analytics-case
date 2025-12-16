-- Snapshot de performance por contrato na data de corte.
-- Grão: loan_id (1 linha por contrato)

with parcelas as (
  select
    loan_id,
    max(case when valor_em_aberto > 0 and data_vencimento < data_corte then dpd_efetivo else 0 end) as dpd_atual,
    sum(valor_em_aberto) as saldo_em_aberto_aprox,
    sum(case when dq_parcela_sem_registro_pagamento then 1 else 0 end) as parcelas_sem_registro,
    max(case when dpd_efetivo >= 30 then 1 else 0 end) as ever_30,
    max(case when dpd_efetivo >= 90 then 1 else 0 end) as ever_90
  from {{ ref('int_installments') }}
  group by 1
),

loans as (
  select
    loan_id,
    customer_id,
    data_concessao,
    valor_contratado,
    prazo_meses,
    taxa_juros_anual,
    status_reportado
  from {{ ref('stg_loans') }}
),

data_corte as (
  -- Evitamos o nome "asof" para não conflitar com ASOF JOIN do DuckDB.
  select
    coalesce(try_cast('{{ var("data_corte", "") }}' as date), current_date) as data_corte
),

final as (
  select
    l.loan_id,
    l.customer_id,
    l.data_concessao,
    l.valor_contratado,
    l.prazo_meses,
    l.taxa_juros_anual,
    l.status_reportado,

    p.dpd_atual,
    p.saldo_em_aberto_aprox,
    p.parcelas_sem_registro,
    p.ever_30,
    p.ever_90,

    dc.data_corte,

    case
      when p.saldo_em_aberto_aprox = 0 then 'liquidado'
      when p.dpd_atual >= 90 then 'inadimplente'
      else 'ativo'
    end as status_derivado,

    case
      when lower(l.status_reportado) <> lower(
        case
          when p.saldo_em_aberto_aprox = 0 then 'liquidado'
          when p.dpd_atual >= 90 then 'inadimplente'
          else 'ativo'
        end
      )
      then true else false
    end as dq_status_divergente
  from loans l
  left join parcelas p
    on p.loan_id = l.loan_id
  cross join data_corte dc
)

select * from final
