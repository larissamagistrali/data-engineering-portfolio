"""Silver: tipagem, regras de qualidade com contagem por regra, deduplicação e enriquecimento com zonas."""
from __future__ import annotations

from dataclasses import dataclass, field

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

# Coluna original da TLC -> (nome na silver, tipo). Nomes da TLC variam em caixa; a busca é sem diferenciar maiúsculas.
SOURCE_MAP: dict[str, tuple[str, str]] = {
    "VendorID": ("vendor_id", "int"),
    "tpep_pickup_datetime": ("pickup_ts", "timestamp"),
    "tpep_dropoff_datetime": ("dropoff_ts", "timestamp"),
    "passenger_count": ("passenger_count", "int"),
    "trip_distance": ("trip_distance", "double"),
    "RatecodeID": ("rate_code_id", "int"),
    "store_and_fwd_flag": ("store_and_fwd_flag", "string"),
    "PULocationID": ("pu_location_id", "int"),
    "DOLocationID": ("do_location_id", "int"),
    "payment_type": ("payment_type", "int"),
    "fare_amount": ("fare_amount", "double"),
    "extra": ("extra", "double"),
    "mta_tax": ("mta_tax", "double"),
    "tip_amount": ("tip_amount", "double"),
    "tolls_amount": ("tolls_amount", "double"),
    "improvement_surcharge": ("improvement_surcharge", "double"),
    "total_amount": ("total_amount", "double"),
    "congestion_surcharge": ("congestion_surcharge", "double"),
    "Airport_fee": ("airport_fee", "double"),
}
TRIP_COLUMNS = [dst for dst, _ in SOURCE_MAP.values()]
MAX_DISTANCE_MILES = 500.0  # teto de sanidade; viagens acima disso são erro de medição
MAX_FARE_USD = 1000.0
MAX_PASSENGERS = 6
DOW_PT = ["Dom", "Seg", "Ter", "Qua", "Qui", "Sex", "Sáb"]  # dayofweek: 1 = domingo


@dataclass
class Rule:
    name: str
    description: str
    rejects: Column  # verdadeiro quando a linha deve ser rejeitada


def quality_rules() -> list[Rule]:
    """Ordem importa: cada linha rejeitada conta só na primeira regra que ela viola."""
    pu, do = F.col("pickup_ts"), F.col("dropoff_ts")
    return [
        Rule("null_timestamp", "pickup ou dropoff nulo", pu.isNull() | do.isNull()),
        Rule("dropoff_before_pickup", "dropoff anterior ao pickup", do < pu),
        Rule(
            "outside_source_month",
            "pickup fora do mês do arquivo de origem",
            F.date_format(pu, "yyyy-MM") != F.col("source_month"),
        ),
        Rule(
            "invalid_distance",
            f"distância nula, zero ou acima de {MAX_DISTANCE_MILES:g} milhas",
            F.col("trip_distance").isNull()
            | (F.col("trip_distance") <= 0)
            | (F.col("trip_distance") > MAX_DISTANCE_MILES),
        ),
        Rule(
            "invalid_fare",
            f"tarifa nula, zero ou acima de US$ {MAX_FARE_USD:g}",
            F.col("fare_amount").isNull() | (F.col("fare_amount") <= 0) | (F.col("fare_amount") > MAX_FARE_USD),
        ),
        Rule("invalid_total", "valor total negativo ou nulo", F.col("total_amount").isNull() | (F.col("total_amount") < 0)),
        Rule(
            "invalid_passengers",
            f"passageiros nulos, zero ou acima de {MAX_PASSENGERS}",
            F.col("passenger_count").isNull()
            | (F.col("passenger_count") < 1)
            | (F.col("passenger_count") > MAX_PASSENGERS),
        ),
    ]


def type_trips(bronze: DataFrame) -> DataFrame:
    """Seleciona e tipa as colunas da TLC; coluna ausente vira nula (tolerante a variações de schema)."""
    lower = {c.lower(): c for c in bronze.columns}
    cols = []
    for src, (dst, typ) in SOURCE_MAP.items():
        real = lower.get(src.lower())
        expr = F.col(f"`{real}`") if real else F.lit(None)
        cols.append(expr.cast(typ).alias(dst))
    return bronze.select(*cols, "source_file", "source_month", "ingested_at")


def apply_rules(typed: DataFrame) -> DataFrame:
    """Marca em `reject_rule` a primeira regra violada (nula quando a linha é válida)."""
    rules = quality_rules()
    reason = F.coalesce(*[F.when(r.rejects, F.lit(r.name)) for r in rules])
    return typed.withColumn("reject_rule", reason)


@dataclass
class SilverStats:
    bronze_rows: int = 0
    rejected_by_rule: dict[str, int] = field(default_factory=dict)
    duplicate_rows: int = 0
    silver_rows: int = 0
    bronze_by_month: dict[str, int] = field(default_factory=dict)
    silver_by_month: dict[str, int] = field(default_factory=dict)

    @property
    def rejected_total(self) -> int:
        return sum(self.rejected_by_rule.values()) + self.duplicate_rows


def validate_and_dedupe(typed: DataFrame) -> tuple[DataFrame, SilverStats]:
    """Aplica regras, conta rejeições por regra, remove duplicatas exatas e devolve as linhas válidas."""
    stats = SilverStats()
    flagged = apply_rules(typed).cache()

    stats.bronze_rows = flagged.count()
    stats.bronze_by_month = {r["source_month"]: r["count"] for r in flagged.groupBy("source_month").count().collect()}
    by_rule = {r["reject_rule"]: r["count"] for r in flagged.groupBy("reject_rule").count().collect()}
    stats.rejected_by_rule = {r.name: by_rule.get(r.name, 0) for r in quality_rules()}

    valid = flagged.filter(F.col("reject_rule").isNull()).drop("reject_rule").cache()
    deduped = valid.dropDuplicates(TRIP_COLUMNS).cache()
    n_valid, n_dedup = valid.count(), deduped.count()
    stats.duplicate_rows = n_valid - n_dedup
    stats.silver_rows = n_dedup
    stats.silver_by_month = {
        r["source_month"]: r["count"] for r in deduped.groupBy("source_month").count().collect()
    }
    valid.unpersist()
    flagged.unpersist()
    if stats.bronze_rows != stats.silver_rows + sum(stats.rejected_by_rule.values()) + stats.duplicate_rows:
        raise RuntimeError("contagem de linhas não fecha: bronze != silver + rejeitadas + duplicadas")
    return deduped, stats


def enrich(trips: DataFrame, zones: DataFrame) -> DataFrame:
    """Junta nome de zona e bairro de embarque/desembarque e cria colunas derivadas."""
    pu_zone = zones.select(
        F.col("location_id").alias("pu_location_id"),
        F.col("borough").alias("pu_borough"),
        F.col("zone").alias("pu_zone"),
    )
    do_zone = zones.select(
        F.col("location_id").alias("do_location_id"),
        F.col("borough").alias("do_borough"),
        F.col("zone").alias("do_zone"),
    )
    joined = (
        trips.join(F.broadcast(pu_zone), "pu_location_id", "left")
        .join(F.broadcast(do_zone), "do_location_id", "left")
        .withColumn("pu_zone", F.coalesce("pu_zone", F.lit("Zona não mapeada")))
        .withColumn("do_zone", F.coalesce("do_zone", F.lit("Zona não mapeada")))
        .withColumn("pu_borough", F.coalesce("pu_borough", F.lit("Não mapeado")))
        .withColumn("do_borough", F.coalesce("do_borough", F.lit("Não mapeado")))
    )
    return (
        joined.withColumn("pickup_month", F.date_format("pickup_ts", "yyyy-MM"))
        .withColumn("trip_date", F.to_date("pickup_ts"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        .withColumn("pickup_dow", F.dayofweek("pickup_ts"))
        .withColumn("dow_name", F.element_at(F.array(*[F.lit(d) for d in DOW_PT]), F.dayofweek("pickup_ts")))
        .withColumn(
            "trip_minutes",
            (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60.0,
        )
    )


def build_silver(bronze: DataFrame, zones: DataFrame) -> tuple[DataFrame, SilverStats]:
    """Bronze -> silver: tipagem, regras, deduplicação e enriquecimento."""
    trips, stats = validate_and_dedupe(type_trips(bronze))
    return enrich(trips, zones), stats
