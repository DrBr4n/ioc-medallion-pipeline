from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as sf

GRAIN: list[str] = ["athlete_id", "games", "sport", "event"]


def participation_id(grain: list[str]) -> Column:
    parts: list[Column] = [
        sf.coalesce(sf.col(c).cast("string"), sf.lit("\u0000")) for c in grain
    ]
    return sf.sha2(sf.concat_ws("||", *parts), 256)


def clean(df: DataFrame) -> DataFrame:
    typed: DataFrame = df.select(
        sf.col("ID").cast("long").alias("athlete_id"),
        sf.trim("Name").alias("name"),
        sf.upper(sf.trim("Sex")).alias("sex"),
        sf.col("Age").cast("int").alias("age"),
        sf.col("Height").cast("double").alias("height_cm"),
        sf.col("Weight").cast("double").alias("weight_kg"),
        sf.trim("Team").alias("team"),
        sf.upper(sf.trim("NOC")).alias("noc"),
        sf.trim("Games").alias("games"),
        sf.col("Year").cast("int").alias("year"),
        sf.initcap(sf.trim("Season")).alias("season"),
        sf.trim("City").alias("city"),
        sf.trim("Sport").alias("sport"),
        sf.trim("Event").alias("event"),
        sf.when(sf.col("Medal") == sf.lit("NA"), None)
            .otherwise(sf.trim("Medal"))
            .alias("medal"),
        sf.col("load_ts")
    )

    keyed: DataFrame = typed.withColumn("participation_id", participation_id(GRAIN))

    return keyed.filter(sf.col("athlete_id").isNotNull()).dropDuplicates(
        ["participation_id"]
    )


def clean_regions(df: DataFrame) -> DataFrame:
    clean: DataFrame = df.select(
        sf.upper(sf.trim("NOC")).alias("noc"),
        sf.trim("region").alias("region"),
        sf.trim("notes").alias("notes"),
        sf.col("load_ts")
    )
    return clean.filter(sf.col("noc").isNotNull()).dropDuplicates(["noc"])
