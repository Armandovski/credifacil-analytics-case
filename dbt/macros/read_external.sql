{% macro ext_parquet(source_name, table_name) %}
  {#
    Leitura de fontes Bronze (Parquet) no DuckDB.

    IMPORTANTÍSSIMO:
    - Este macro precisa retornar uma RELAÇÃO (ex.: read_parquet(...)),
      e NÃO um "select ...", porque ele será usado dentro de "FROM {{ ... }}".
  #}

  {% set base_dir = env_var('BRONZE_DIR', './data/bronze') %}
  {% set pasta = table_name | replace('_raw', '') %}
  {% set caminho = base_dir ~ '/' ~ pasta ~ '/**/*.parquet' %}

  read_parquet('{{ caminho }}', hive_partitioning=1)
{% endmacro %}

