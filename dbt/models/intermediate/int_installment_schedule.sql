with loans as (
  select
    loan_id,
    customer_id,
    data_concessao,
    prazo_meses,
    status_reportado,
    pmt_teorica
  from {{ ref('stg_loans') }}
),

schedule as (
  select
    l.loan_id,
    l.customer_id,
    l.data_concessao,
    l.prazo_meses,
    l.status_reportado,
    l.pmt_teorica,

    n as numero_parcela,

    -- DuckDB: DATE + INTEGER OK, DATE + BIGINT NÃO.
    -- range() retorna BIGINT, então forçamos para INTEGER.
    (l.data_concessao + cast(n * 30 as integer))::date as data_vencimento

  from loans l
  join range(1, l.prazo_meses + 1) as r(n) on true
)

select * from schedule
