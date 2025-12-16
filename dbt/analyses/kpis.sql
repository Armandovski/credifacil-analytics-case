-- Consultas exemplo de KPIs (a partir dos marts Gold).

-- 1) Taxa de inadimplência (90+)
select
  count(*) filter (where atraso_atual_dias >= 90) * 1.0
  / nullif(count(*) filter (where status_derivado in ('ativo','inadimplente')), 0) as taxa_inadimplencia_90
from mart_credit_loans;

-- 2) PAR30 e PAR90 (usando saldo_em_aberto como proxy de exposição)
select
  sum(case when atraso_atual_dias > 30 then saldo_em_aberto else 0 end) / nullif(sum(saldo_em_aberto),0) as par30,
  sum(case when atraso_atual_dias > 90 then saldo_em_aberto else 0 end) / nullif(sum(saldo_em_aberto),0) as par90
from mart_credit_loans
where status_derivado in ('ativo','inadimplente');

-- 3) Vintage (inadimplência por mês de originação)
select
  date_trunc('month', data_concessao) as mes_originacao,
  count(*) as qtd_contratos,
  sum(case when atraso_atual_dias >= 90 then 1 else 0 end) as qtd_inadimplentes_90,
  sum(case when atraso_atual_dias >= 90 then 1 else 0 end) * 1.0 / count(*) as taxa_inadimplencia_90
from mart_credit_loans
group by 1
order by 1;

-- 4) Share de clientes recorrentes
select
  sum(case when cliente_recorrente then 1 else 0 end) * 1.0 / nullif(count(*),0) as pct_clientes_recorrentes
from (
  select distinct customer_id, cliente_recorrente
  from mart_credit_loans
) t;
