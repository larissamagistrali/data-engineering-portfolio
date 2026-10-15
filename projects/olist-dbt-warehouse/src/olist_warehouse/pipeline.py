"""Orquestra o pipeline local: gera/carrega o raw, roda `dbt build`, gera docs e exporta a gold.

Uso: ``python -m olist_warehouse.pipeline [--orders 20000] [--skip-sample]``
Requer dbt-duckdb instalado (veja requirements.txt do projeto).
"""
from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path

from olist_warehouse import export_gold, load_raw, make_sample

PROJECT_DIR = Path(__file__).resolve().parents[2]
DBT_DIR = PROJECT_DIR / "dbt"


def run_dbt(args: list[str]) -> bool:
    """Executa um comando dbt dentro de dbt/ (em subprocesso, para liberar o arquivo DuckDB ao terminar)."""
    env = {**os.environ, "OLIST_DUCKDB": str(PROJECT_DIR / "data" / "raw" / "olist.duckdb")}
    cmd = [sys.executable, "-c", "from dbt.cli.main import cli; cli()", *args,
           "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    return subprocess.run(cmd, env=env, check=False).returncode == 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--orders", type=int, default=20000)
    ap.add_argument("--skip-sample", action="store_true", help="reutiliza os CSVs já existentes em data/raw")
    args = ap.parse_args()

    raw_dir = PROJECT_DIR / "data" / "raw"
    if not args.skip_sample:
        make_sample.write_csvs(make_sample.generate(args.orders), raw_dir)
    counts = load_raw.load_raw(raw_dir, raw_dir / "olist.duckdb")
    print("raw:", counts)
    ok = run_dbt(["build"])
    run_dbt(["docs", "generate", "--no-compile"])  # catalog.json (tipos das colunas)
    print(export_gold.export_all())
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
