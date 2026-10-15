"""Gold: agregados prontos para o app (somas reagregáveis; médias ponderadas quando possível)."""
from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from . import config


def _grouped(silver: DataFrame, keys: list[str]) -> DataFrame:
    aggs = [
        F.count("*").alias("trips"),
        F.sum("total_amount").alias("revenue_total"),
        F.sum("fare_amount").alias("fare_total"),
        F.sum("tip_amount").alias("tip_total"),
    ]
    return silver.groupBy(*keys).agg(*aggs).withColumn(
        "tip_pct", F.round(F.col("tip_total") / F.col("fare_total") * 100, 3)
    )


def _distribution(silver: DataFrame, metric: str, value: str, width: float, cap: float, unit: str) -> DataFrame:
    n_buckets = int(cap / width)
    fmt = "%.1f" if width < 1 else "%.0f"
    bucket = F.least(F.floor(F.col(value) / width), F.lit(n_buckets)).cast("int")
    start = F.round(bucket * width, 2)
    label = F.when(bucket >= n_buckets, F.lit(f"{cap:g}+ {unit}")).otherwise(
        F.format_string(f"{fmt}–{fmt} {unit}", start, start + F.lit(width))
    )
    return (
        silver.withColumn("bucket_start", start)
        .withColumn("bucket_label", label)
        .groupBy("bucket_start", "bucket_label")
        .agg(F.count("*").alias("trips"))
        .withColumn("metric", F.lit(metric))
        .orderBy("bucket_start")
    )


def build_gold(silver: DataFrame) -> dict[str, DataFrame]:
    """Dicionário nome_da_tabela -> DataFrame Spark, prontos para exportar."""
    overview = silver.agg(
        F.count("*").alias("trips"),
        F.sum("total_amount").alias("revenue_total"),
        F.avg("fare_amount").alias("avg_fare"),
        F.sum("tip_amount").alias("tip_total"),
        F.sum("fare_amount").alias("fare_total"),
        F.avg("trip_minutes").alias("avg_duration_min"),
        F.avg("trip_distance").alias("avg_distance_mi"),
        F.min("trip_date").alias("first_date"),
        F.max("trip_date").alias("last_date"),
        F.countDistinct("pu_location_id").alias("pickup_zones"),
    ).withColumn("tip_pct", F.round(F.col("tip_total") / F.col("fare_total") * 100, 3))

    daily = _grouped(silver, ["trip_date"]).join(
        silver.groupBy("trip_date").agg(
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_minutes").alias("avg_duration_min"),
            F.avg("trip_distance").alias("avg_distance_mi"),
        ),
        "trip_date",
    )
    hourly = _grouped(silver, ["trip_date", "pickup_hour"])
    heatmap = _grouped(silver, ["pickup_dow", "dow_name", "pickup_hour"])
    zone = _grouped(silver, ["pu_location_id", "pu_zone", "pu_borough"]).join(
        silver.groupBy("pu_location_id").agg(F.avg("trip_distance").alias("avg_distance_mi")), "pu_location_id"
    )
    zone_hour = _grouped(silver, ["pu_location_id", "pu_zone", "pu_borough", "pickup_hour"])
    od_pairs = (
        silver.groupBy("pu_zone", "do_zone")
        .agg(
            F.count("*").alias("trips"),
            F.avg("trip_distance").alias("avg_distance_mi"),
            F.avg("trip_minutes").alias("avg_duration_min"),
        )
        .orderBy(F.desc("trips"), "pu_zone", "do_zone")
        .limit(config.TOP_OD_PAIRS)
    )
    distributions = _distribution(
        silver, "duracao", "trip_minutes", 5.0, config.MAX_DURATION_MIN, "min"
    ).unionByName(_distribution(silver, "distancia", "trip_distance", 0.5, config.MAX_DISTANCE_MI, "mi"))
    return {
        "gold_overview": overview,
        "gold_daily": daily,
        "gold_hourly": hourly,
        "gold_heatmap_hour_dow": heatmap,
        "gold_zone": zone,
        "gold_zone_hour": zone_hour,
        "gold_od_top": od_pairs,
        "gold_distributions": distributions,
    }
