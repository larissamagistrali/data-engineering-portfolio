"""Bronze: ingestão fiel dos arquivos brutos, sem limpeza, com metadados de origem."""
from __future__ import annotations

import re
from datetime import datetime, timezone
from functools import reduce
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


def month_from_filename(name: str) -> str:
    """'yellow_tripdata_2024-01.parquet' -> '2024-01'."""
    match = re.search(r"(\d{4})-(\d{2})", name)
    if not match:
        raise ValueError(f"não encontrei AAAA-MM no nome do arquivo: {name}")
    return f"{match.group(1)}-{match.group(2)}"


def ingest_bronze(spark: SparkSession, files: list[Path], out_dir: Path) -> DataFrame:
    """Lê os Parquets da TLC, acrescenta source_file, source_month e ingested_at e grava particionado por mês."""
    ingested_at = datetime.now(timezone.utc).replace(tzinfo=None)
    frames = []
    for path in files:
        frame = spark.read.parquet(str(path))
        frames.append(
            frame.withColumn("source_file", F.lit(path.name))
            .withColumn("source_month", F.lit(month_from_filename(path.name)))
            .withColumn("ingested_at", F.lit(ingested_at).cast("timestamp"))
        )
    bronze = reduce(lambda a, b: a.unionByName(b), frames)
    bronze.write.mode("overwrite").partitionBy("source_month").parquet(str(out_dir))
    return spark.read.parquet(str(out_dir))
