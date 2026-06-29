from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as sf

from src.transform.scd import apply_scd1, apply_scd2


def latest_per_key(df: DataFrame, key_cols: list[str]) -> DataFrame:
    """Keep the newest row per key by ``load_ts``, then drop ``load_ts``."""

    window = Window.partitionBy(*key_cols).orderBy(sf.col("load_ts").desc())
    return (
        df.withColumn("_rn", sf.row_number().over(window))
        .filter(sf.col("_rn") == 1)
        .drop("_rn", "load_ts")
    )


def build_dim_athlete(
    df: DataFrame, stored: DataFrame | None, as_of_date: str
) -> DataFrame:
    snapshot: DataFrame = latest_per_key(
        df.select("athlete_id", "name", "sex", "load_ts"), ["athlete_id"]
    )
    return apply_scd2(
        stored,
        snapshot,
        business_key="athlete_id",
        tracked_columns=["name"],
        surrogate_key="athlete_key",
        as_of_date=as_of_date,
    )


def build_dim_games(df: DataFrame, stored: DataFrame | None) -> DataFrame:
    snapshot: DataFrame = df.select(
        "games", "year", "season", "city"
    ).dropDuplicates(["games"])
    return apply_scd1(stored, snapshot, business_key="games", surrogate_key="games_key")


def build_dim_event(df: DataFrame, stored: DataFrame | None) -> DataFrame:
    snapshot: DataFrame = (
        df.withColumn(
            "event_nk", sf.concat_ws(" | ", sf.col("sport"), sf.col("event"))
        )
        .select("event_nk", "sport", "event")
        .dropDuplicates(["event_nk"])
    )
    return apply_scd1(
        stored, snapshot, business_key="event_nk", surrogate_key="event_key"
    )


def build_dim_noc(
    df: DataFrame,
    regions: DataFrame | None,
    stored: DataFrame | None,
) -> DataFrame:
    noc_keys: DataFrame = df.select("noc").dropDuplicates(["noc"])
    if regions is not None:
        ref: DataFrame = regions.select("noc", "region", "notes")
        snapshot: DataFrame = noc_keys.join(ref, on="noc", how="left")
    else:
        snapshot = noc_keys.withColumn(
            "region", sf.lit(None).cast("string")
        ).withColumn("notes", sf.lit(None).cast("string"))
    return apply_scd1(stored, snapshot, business_key="noc", surrogate_key="noc_key")


def build_fact(
    df: DataFrame,
    dim_athlete: DataFrame,
    dim_games: DataFrame,
    dim_event: DataFrame,
    dim_noc: DataFrame,
) -> DataFrame:
    enriched: DataFrame = df.withColumn(
        "event_nk", sf.concat_ws(" | ", sf.col("sport"), sf.col("event"))
    )

    athlete_cur: DataFrame = dim_athlete.filter(sf.col("is_current")).select(
        "athlete_id", "athlete_key"
    )

    joined: DataFrame = (
        enriched.join(athlete_cur, on="athlete_id", how="left")
        .join(dim_games.select("games", "games_key"), on="games", how="left")
        .join(dim_event.select("event_nk", "event_key"), on="event_nk", how="left")
        .join(dim_noc.select("noc", "noc_key"), on="noc", how="left")
    )

    return joined.select(
        sf.col("participation_id"),
        sf.col("athlete_key"),
        sf.col("games_key"),
        sf.col("event_key"),
        sf.col("noc_key"),
        sf.col("team"),
        sf.col("year"),
        sf.col("age"),
        sf.col("height_cm"),
        sf.col("weight_kg"),
        sf.col("medal"),
        sf.col("medal").isNotNull().alias("medal_won"),
        (sf.col("medal") == sf.lit("Gold")).cast("int").alias("gold_count"),
        (sf.col("medal") == sf.lit("Silver")).cast("int").alias("silver_count"),
        (sf.col("medal") == sf.lit("Bronze")).cast("int").alias("bronze_count"),
        sf.col("load_ts"),
    )
