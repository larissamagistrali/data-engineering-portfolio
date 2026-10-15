"""Sessão Spark local do projeto: sem interface web, sem metastore, fuso UTC."""
from __future__ import annotations

from pathlib import Path

from pyspark.sql import SparkSession


def build_spark(warehouse_dir: Path, cores: int = 4, app_name: str = "nyc-taxi-lakehouse") -> SparkSession:
    """Spark em modo local. UTC evita que a hora do dia mude com o horário de verão."""
    spark = (
        SparkSession.builder.appName(app_name)
        .master(f"local[{cores}]")
        .config("spark.driver.memory", "3g")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.parquet.compression.codec", "zstd")
        .config("spark.sql.warehouse.dir", str(warehouse_dir))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
