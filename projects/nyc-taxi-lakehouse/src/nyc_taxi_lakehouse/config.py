"""Caminhos e constantes do pipeline (tudo relativo à pasta do projeto)."""
from __future__ import annotations

from pathlib import Path

PROJECT_DIR = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_DIR / "data"  # versionado: só o gold pequeno e o relatório de qualidade
RAW_DIR = DATA_DIR / "raw" / "tlc"  # gitignored: arquivos brutos da TLC
LAKE_DIR = DATA_DIR / "raw" / "lake"  # gitignored: camadas bronze e silver

BRONZE_DIR = LAKE_DIR / "bronze" / "yellow_trips"
SILVER_DIR = LAKE_DIR / "silver" / "yellow_trips"
SPARK_WAREHOUSE_DIR = LAKE_DIR / "spark-warehouse"

MONTHS = ("2024-01", "2024-02")
ZONES_FILE = RAW_DIR / "taxi_zone_lookup.csv"
QUALITY_REPORT = DATA_DIR / "quality_report.json"

SOURCE_URLS = {
    "yellow_tripdata_2024-01.parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet",
    "yellow_tripdata_2024-02.parquet": "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet",
    "taxi_zone_lookup.csv": "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv",
}

TOP_OD_PAIRS = 20
MAX_DURATION_MIN = 60  # acima disso, um único bucket "60+"
MAX_DISTANCE_MI = 20.0  # acima disso, um único bucket "20+"
