"""Pipeline completo: bronze -> silver -> gold, relatório de qualidade e exportação do gold para data/.

Execução: PYTHONPATH=projects/nyc-taxi-lakehouse/src python -m nyc_taxi_lakehouse.pipeline
"""
from __future__ import annotations

import json
import platform
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path

from . import config
from .bronze import ingest_bronze
from .gold import build_gold
from .silver import build_silver, quality_rules

# pyspark avisa sobre pandas 3 a cada toPandas; o aviso não afeta o resultado.
warnings.filterwarnings("ignore", message="PySpark does not yet fully support pandas")


def check_inputs() -> list[Path]:
    """Confere os arquivos brutos; se faltar algum, avisa de onde baixar (não baixa automaticamente)."""
    files = [config.RAW_DIR / f"yellow_tripdata_{m}.parquet" for m in config.MONTHS]
    missing = [p for p in [*files, config.ZONES_FILE] if not p.exists()]
    if missing:
        hints = "\n".join(f"  {p.name}: {config.SOURCE_URLS[p.name]}" for p in missing)
        raise FileNotFoundError(f"arquivos ausentes em {config.RAW_DIR}:\n{hints}")
    return files


def export_gold(tables: dict[str, object]) -> dict[str, int]:
    """Grava cada tabela gold como Parquet zstd em data/ e devolve o número de linhas."""
    rows = {}
    for name, frame in tables.items():
        pdf = frame.toPandas()  # type: ignore[attr-defined]
        for col in pdf.select_dtypes(include="float").columns:
            pdf[col] = pdf[col].round(3)
        pdf.to_parquet(config.DATA_DIR / f"{name}.parquet", compression="zstd", index=False)
        rows[name] = len(pdf)
    return rows


def run() -> dict:
    from .session import build_spark

    files = check_inputs()
    config.LAKE_DIR.mkdir(parents=True, exist_ok=True)
    config.DATA_DIR.mkdir(parents=True, exist_ok=True)
    started = datetime.now(timezone.utc)
    timings: dict[str, float] = {}

    spark = build_spark(config.SPARK_WAREHOUSE_DIR)
    try:
        java_version = spark.sparkContext._jvm.java.lang.System.getProperty("java.version")  # type: ignore[attr-defined]
        zones = (
            spark.read.option("header", True).csv(str(config.ZONES_FILE))
            .selectExpr(
                "cast(LocationID as int) as location_id",
                "Borough as borough",
                "Zone as zone",
                "service_zone",
            )
        )

        t = time.perf_counter()
        bronze = ingest_bronze(spark, files, config.BRONZE_DIR)
        timings["bronze"] = round(time.perf_counter() - t, 1)

        t = time.perf_counter()
        silver_df, stats = build_silver(bronze, zones)
        silver_df.write.mode("overwrite").partitionBy("pickup_month").parquet(str(config.SILVER_DIR))
        silver = spark.read.parquet(str(config.SILVER_DIR))
        unmapped = silver.filter("pu_zone = 'Zona não mapeada'").count()
        timings["silver"] = round(time.perf_counter() - t, 1)

        t = time.perf_counter()
        tables = build_gold(silver)
        gold_rows = export_gold(tables)
        timings["gold"] = round(time.perf_counter() - t, 1)
        spark_version = spark.version
        rule_defs = [(r.name, r.description) for r in quality_rules()]
    finally:
        spark.stop()

    report = {
        "dataset": "NYC TLC Yellow Taxi Trip Records",
        "source": "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page",
        "months": list(config.MONTHS),
        "format": "parquet (zstd)",
        "delta": False,
        "totals": {
            "bronze_rows": stats.bronze_rows,
            "rejected_rows": stats.rejected_total,
            "silver_rows": stats.silver_rows,
            "silver_share_pct": round(stats.silver_rows / stats.bronze_rows * 100, 2),
            "unmapped_pickup_zone_rows": unmapped,
        },
        "rules": [
            {
                "rule": name,
                "description": desc,
                "rows_rejected": stats.rejected_by_rule[name],
            }
            for name, desc in rule_defs
        ]
        + [
            {
                "rule": "duplicate_row",
                "description": "linha idêntica (todas as colunas da viagem) a outra já aceita",
                "rows_rejected": stats.duplicate_rows,
            }
        ],
        "by_month": {
            m: {"bronze_rows": stats.bronze_by_month.get(m, 0), "silver_rows": stats.silver_by_month.get(m, 0)}
            for m in config.MONTHS
        },
        "gold_rows": gold_rows,
        "run": {
            "started_utc": started.isoformat(timespec="seconds"),
            "duration_s": {**timings, "total": round(sum(timings.values()), 1)},
            "spark_version": spark_version,
            "java_version": java_version,
            "python_version": platform.python_version(),
            "master": "local[4]",
            "driver_memory": "3g",
        },
    }
    config.QUALITY_REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return report


def main() -> None:
    report = run()
    print(json.dumps(report["totals"], ensure_ascii=False, indent=2))
    print("gold:", report["gold_rows"])
    print("duração (s):", report["run"]["duration_s"])


if __name__ == "__main__":
    main()
