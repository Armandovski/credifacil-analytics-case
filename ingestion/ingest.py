"""
Ingestão (Bronze) — Credifácil.

Este script simula a etapa de ingestão em um Data Lake:
- Recebe CSVs na pasta /app/input (montada via volume no Docker).
- Grava a camada Bronze em Parquet (particionada por ingestion_date).
- Mantém a Bronze o mais "raw" possível, evitando tipar agressivamente.

Importante:
- Para evitar problemas com dados sujos (ex.: 'prazo_meses' = ' 12 '),
  lemos tudo como texto (dtype=str) e deixamos a tipagem/limpeza para o Silver (dbt/SQL).
"""

from __future__ import annotations

import os
import sys
import datetime as dt
from pathlib import Path
from typing import Dict

import pandas as pd
from dotenv import load_dotenv


def _hoje_str() -> str:
    """Retorna a data de hoje em ISO (YYYY-MM-DD)."""
    return dt.date.today().isoformat()


def _ler_csv_como_texto(caminho: Path) -> pd.DataFrame:
    """
    Lê CSV garantindo que TODAS as colunas entrem como texto.

    Isso evita que o pandas infira tipos errados e quebre com espaços, nulos,
    caracteres inesperados, etc. (típico em ingestão de lake).
    """
    return pd.read_csv(
        caminho,
        dtype=str,                # tudo como texto
        keep_default_na=False,    # evita transformar strings vazias em NaN
        na_values=[],             # explicitamente vazio
    )


def _alerta_prazo_meses_com_espaco(df_loans: pd.DataFrame) -> None:
    """
    Alerta se encontrar valores com espaços em 'prazo_meses'.

    Não corrige aqui (Bronze é raw), apenas avisa. A correção é feita no Silver
    com trim() + try_cast().
    """
    if "prazo_meses" not in df_loans.columns:
        return

    serie = df_loans["prazo_meses"].astype(str)
    tem_espaco = serie.str.contains(r"^\s+|\s+$", regex=True).any()
    if tem_espaco:
        print(
            "AVISO: coluna 'prazo_meses' contém valores com espaços em branco "
            "(ex.: ' 12 '). A tipagem/limpeza será tratada no Silver (dbt/SQL).",
            file=sys.stderr,
        )


def _gravar_parquet_particionado(
    df: pd.DataFrame,
    destino_base: Path,
    nome_tabela: str,
    ingestion_date: str,
) -> Path:
    """
    Grava Parquet na Bronze em estrutura estilo Hive:

      bronze/<tabela>/ingestion_date=YYYY-MM-DD/<tabela>.parquet
    """
    destino = destino_base / nome_tabela / f"ingestion_date={ingestion_date}"
    destino.mkdir(parents=True, exist_ok=True)

    arquivo = destino / f"{nome_tabela}.parquet"
    df.to_parquet(arquivo, index=False)
    return arquivo


def main() -> int:
    """
    Ponto de entrada do container.

    Lê input/*.csv e materializa Bronze em data/bronze/*/*.parquet
    """
    load_dotenv()

    data_dir = Path(os.getenv("DATA_DIR", "./data"))
    pasta_input = Path(os.getenv("INPUT_DIR", "./input"))
    pasta_bronze = Path(os.getenv("BRONZE_DIR", data_dir / "bronze"))

    ingestion_date = os.getenv("INGESTION_DATE", _hoje_str())

    # Mapeamento de arquivos esperados
    arquivos: Dict[str, Path] = {
        "customers": pasta_input / "customers.csv",
        "loans": pasta_input / "loans.csv",
        "payments": pasta_input / "payments.csv",
    }

    print("[1/3] Ingestão: CSV -> Parquet (camada Bronze)...")

    for tabela, caminho in arquivos.items():
        if not caminho.exists():
            print(f"ERRO: arquivo não encontrado: {caminho}", file=sys.stderr)
            return 2

        df = _ler_csv_como_texto(caminho)

        if tabela == "loans":
            _alerta_prazo_meses_com_espaco(df)

        arquivo = _gravar_parquet_particionado(
            df=df,
            destino_base=pasta_bronze,
            nome_tabela=tabela,
            ingestion_date=ingestion_date,
        )
        print(f"Bronze gravado em Parquet: {arquivo}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
