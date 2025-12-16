"""Exportação dos marts (Gold) para CSV.

Em um cenário real, os marts seriam consumidos diretamente via BI/Notebook/Feature Store.
Aqui, exportamos para CSV para facilitar a inspeção e entrega do case.
"""

import os
from pathlib import Path

import duckdb
from dotenv import load_dotenv

load_dotenv()


def main() -> None:
    caminho_duckdb = os.getenv("DUCKDB_PATH", "./data/warehouse.duckdb")
    diretorio_saida = Path(os.getenv("OUTPUT_DIR", "./output"))
    diretorio_saida.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(caminho_duckdb, read_only=True)

    exports = {
        "mart_credit_installments": diretorio_saida / "mart_credit_installments.csv",
        "mart_credit_loans": diretorio_saida / "mart_credit_loans.csv",
        "mart_kpis_daily": diretorio_saida / "mart_kpis_daily.csv",
    }

    for nome_tabela, caminho_csv in exports.items():
        df = con.execute(f"select * from {nome_tabela}").df()
        df.to_csv(caminho_csv, index=False)
        print(f"Exportado: {nome_tabela} -> {caminho_csv}")


if __name__ == "__main__":
    main()
