# dbt — Credifácil

Este projeto dbt cria os modelos **Silver** e **Gold** a partir de fontes Parquet na Bronze.

## Camadas
- `staging/` (`stg_*`): tipagem, limpeza, normalização e flags de qualidade
- `intermediate/` (`int_*`): regras de negócio (grade de parcelas, DPD, status derivado)
- `marts/` (`mart_*`): tabelas finais para consumo

## Execução
Na raiz do repositório:
```bash
dbt build --profiles-dir . --project-dir dbt
```
