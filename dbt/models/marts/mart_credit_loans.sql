-- Mart (Gold) no grão de contrato (loan_id).
-- Inclui atributos do contrato + métricas de performance + features simples de cliente (recorrência).

with contratos as (
  select * from {{ ref('int_loan_performance') }}
),

estatisticas_cliente as (
  select
    customer_id,
    count(*) as qtd_contratos_cliente,
    case when count(*) > 1 then true else false end as cliente_recorrente
  from contratos
  group by 1
)

select
  c.*,
  e.qtd_contratos_cliente,
  e.cliente_recorrente
from contratos c
left join estatisticas_cliente e using (customer_id)
