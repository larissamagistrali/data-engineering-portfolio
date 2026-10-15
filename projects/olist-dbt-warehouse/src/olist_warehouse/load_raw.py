"""Carrega os CSVs do Olist (reais ou sintéticos) no schema `raw` do DuckDB, tudo como texto.

É a etapa "EL" do ELT: nada é tipado aqui. A tipagem e a limpeza ficam no dbt (camada staging).
"""
from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

PROJECT_DIR = Path(__file__).resolve().parents[2]

# nome da tabela raw -> nome do arquivo CSV (mesmos nomes do Kaggle)
RAW_FILES = {
    "customers": "olist_customers_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "product_category_name_translation": "product_category_name_translation.csv",
}


def load_raw(raw_dir: Path, db_path: Path) -> dict[str, int]:
    """Recria o schema `raw` a partir dos CSVs e devolve {tabela: nº de linhas}."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    missing = [f for f in RAW_FILES.values() if not (raw_dir / f).exists()]
    if missing:
        raise FileNotFoundError(f"CSVs ausentes em {raw_dir}: {missing}")
    con = duckdb.connect(str(db_path))
    try:
        con.execute("drop schema if exists raw cascade")
        con.execute("create schema raw")
        counts = {}
        for table, fname in RAW_FILES.items():
            path = str((raw_dir / fname).resolve()).replace("'", "''")
            con.execute(
                f"create table raw.{table} as select * from read_csv('{path}', header=true, all_varchar=true)"
            )
            counts[table] = con.execute(f"select count(*) from raw.{table}").fetchone()[0]
        return counts
    finally:
        con.close()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--raw-dir", type=Path, default=PROJECT_DIR / "data" / "raw")
    ap.add_argument("--db", type=Path, default=PROJECT_DIR / "data" / "raw" / "olist.duckdb")
    args = ap.parse_args()
    for table, n in load_raw(args.raw_dir, args.db).items():
        print(f"raw.{table}: {n:,} linhas")


if __name__ == "__main__":
    main()
