{% macro data_referencia() %}
  {#-
    Retorna a data de referência do pipeline.

    - Se o usuário passar --vars '{"data_referencia": "YYYY-MM-DD"}', usa essa data.
    - Caso contrário, usa current_date.

    Observação: usamos ingestion_date como proxy de recência no Bronze; em produção,
    o ideal seria ter updated_at/event_time da fonte.
  -#}

  {%- if var('data_referencia', none) is not none -%}
    try_cast('{{ var("data_referencia") }}' as date)
  {%- else -%}
    current_date
  {%- endif -%}
{% endmacro %}