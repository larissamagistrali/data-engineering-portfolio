"""Testes das regras de qualidade, da deduplicação, do enriquecimento e do gold (Spark local, dados mínimos)."""
from __future__ import annotations

from datetime import datetime

import pytest

pytest.importorskip("pyspark")

from nyc_taxi_lakehouse.bronze import month_from_filename  # noqa: E402

BASE = dict(
    VendorID=1,
    passenger_count=1,
    trip_distance=2.0,
    RatecodeID=1,
    store_and_fwd_flag="N",
    PULocationID=161,
    DOLocationID=236,
    payment_type=1,
    fare_amount=10.0,
    extra=0.0,
    mta_tax=0.5,
    tip_amount=2.0,
    tolls_amount=0.0,
    improvement_surcharge=1.0,
    total_amount=13.5,
    congestion_surcharge=2.5,
    Airport_fee=0.0,
)


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("nyc-taxi-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("wh")))
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


def trip(pickup: datetime, dropoff: datetime | None, month: str = "2024-01", **over) -> dict:
    row = {**BASE, "tpep_pickup_datetime": pickup, "tpep_dropoff_datetime": dropoff, "source_month": month,
           "source_file": f"yellow_tripdata_{month}.parquet", "ingested_at": datetime(2024, 3, 1)}
    row.update(over)
    return row


def make_bronze(spark, rows: list[dict]):
    from pyspark.sql.types import (
        DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType,
    )

    fields = [
        StructField("VendorID", IntegerType()), StructField("tpep_pickup_datetime", StringType()),
        StructField("tpep_dropoff_datetime", StringType()), StructField("passenger_count", LongType()),
        StructField("trip_distance", DoubleType()), StructField("RatecodeID", LongType()),
        StructField("store_and_fwd_flag", StringType()), StructField("PULocationID", IntegerType()),
        StructField("DOLocationID", IntegerType()), StructField("payment_type", LongType()),
        StructField("fare_amount", DoubleType()), StructField("extra", DoubleType()),
        StructField("mta_tax", DoubleType()), StructField("tip_amount", DoubleType()),
        StructField("tolls_amount", DoubleType()), StructField("improvement_surcharge", DoubleType()),
        StructField("total_amount", DoubleType()), StructField("congestion_surcharge", DoubleType()),
        StructField("Airport_fee", DoubleType()), StructField("source_file", StringType()),
        StructField("source_month", StringType()), StructField("ingested_at", TimestampType()),
    ]
    # Timestamps como texto ISO: o cast usa o fuso da sessão (UTC), não o fuso local do Python.
    text_rows = [
        {k: (v.isoformat() if isinstance(v, datetime) and k.startswith("tpep") else v) for k, v in r.items()}
        for r in rows
    ]
    return spark.createDataFrame(text_rows, StructType(fields))


def test_month_from_filename() -> None:
    assert month_from_filename("yellow_tripdata_2024-02.parquet") == "2024-02"
    with pytest.raises(ValueError):
        month_from_filename("sem_data.parquet")


def test_each_rule_rejects_only_its_bad_row(spark) -> None:
    from nyc_taxi_lakehouse.silver import validate_and_dedupe

    good = trip(datetime(2024, 1, 5, 8), datetime(2024, 1, 5, 8, 20))
    rows = [
        good,
        trip(datetime(2024, 1, 5, 9), datetime(2024, 1, 5, 8)),  # dropoff antes do pickup
        trip(datetime(2024, 2, 1, 0, 30), datetime(2024, 2, 1, 0, 50)),  # pickup fora do mês do arquivo
        trip(datetime(2024, 1, 6, 8), datetime(2024, 1, 6, 8, 5), trip_distance=0.0),
        trip(datetime(2024, 1, 6, 9), datetime(2024, 1, 6, 9, 5), fare_amount=-3.0),
        trip(datetime(2024, 1, 6, 10), datetime(2024, 1, 6, 10, 5), passenger_count=0),
        trip(datetime(2024, 1, 6, 11), datetime(2024, 1, 6, 11, 5), passenger_count=None),
    ]
    typed_rows = make_bronze(spark, rows)
    from nyc_taxi_lakehouse.silver import type_trips

    kept, stats = validate_and_dedupe(type_trips(typed_rows))

    assert stats.bronze_rows == 7
    assert stats.rejected_by_rule["dropoff_before_pickup"] == 1
    assert stats.rejected_by_rule["outside_source_month"] == 1
    assert stats.rejected_by_rule["invalid_distance"] == 1
    assert stats.rejected_by_rule["invalid_fare"] == 1
    assert stats.rejected_by_rule["invalid_passengers"] == 2
    assert stats.rejected_by_rule["null_timestamp"] == 0
    assert stats.silver_rows == 1
    assert kept.count() == 1


def test_exact_duplicates_are_removed_and_counted(spark) -> None:
    from nyc_taxi_lakehouse.silver import type_trips, validate_and_dedupe

    same = trip(datetime(2024, 1, 7, 8), datetime(2024, 1, 7, 8, 15))
    other = trip(datetime(2024, 1, 7, 9), datetime(2024, 1, 7, 9, 15), PULocationID=237)
    kept, stats = validate_and_dedupe(type_trips(make_bronze(spark, [same, dict(same), other])))

    assert stats.duplicate_rows == 1
    assert stats.silver_rows == 2
    assert kept.count() == 2


def test_bronze_equals_silver_plus_rejections(spark) -> None:
    """O invariante da contagem fecha: nenhuma linha some sem ser contada."""
    from nyc_taxi_lakehouse.silver import type_trips, validate_and_dedupe

    rows = [
        trip(datetime(2024, 1, 2, 8), datetime(2024, 1, 2, 7)),
        trip(datetime(2024, 1, 2, 8), datetime(2024, 1, 2, 8, 9), trip_distance=900.0),
        trip(datetime(2024, 1, 2, 8), datetime(2024, 1, 2, 8, 9)),
    ]
    _, stats = validate_and_dedupe(type_trips(make_bronze(spark, rows)))
    assert stats.bronze_rows == stats.silver_rows + stats.rejected_total


def test_enrich_and_gold_aggregates(spark) -> None:
    from pyspark.sql import functions as F

    from nyc_taxi_lakehouse.gold import build_gold
    from nyc_taxi_lakehouse.silver import build_silver

    zones = spark.createDataFrame(
        [(161, "Manhattan", "Midtown Center"), (236, "Manhattan", "Upper East Side North")],
        "location_id int, borough string, zone string",
    )
    rows = [
        trip(datetime(2024, 1, 3, 8, 0), datetime(2024, 1, 3, 8, 20), fare_amount=10.0, tip_amount=2.0),
        trip(datetime(2024, 1, 3, 8, 30), datetime(2024, 1, 3, 8, 55), fare_amount=30.0, tip_amount=0.0,
             PULocationID=999),
    ]
    silver, stats = build_silver(make_bronze(spark, rows), zones)
    silver = silver.cache()
    assert stats.silver_rows == 2
    assert sorted(r["trip_minutes"] for r in silver.collect()) == [20.0, 25.0]
    assert {r["pickup_hour"] for r in silver.collect()} == {8}
    assert {r["dow_name"] for r in silver.collect()} == {"Qua"}  # 2024-01-03 foi quarta-feira
    unmapped = silver.filter(F.col("pu_zone") == "Zona não mapeada").count()
    assert unmapped == 1

    tables = build_gold(silver)
    overview = tables["gold_overview"].collect()[0]
    assert overview["trips"] == 2
    assert overview["tip_total"] == pytest.approx(2.0)
    assert overview["fare_total"] == pytest.approx(40.0)
    # gorjeta ponderada = soma(gorjeta) / soma(tarifa) * 100
    assert overview["tip_pct"] == pytest.approx(5.0)
    zone_hour_trips = tables["gold_zone_hour"].agg(F.sum("trips")).collect()[0][0]
    assert zone_hour_trips == 2
