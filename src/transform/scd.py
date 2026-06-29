from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as sf
from pyspark.sql.window import WindowSpec

END_OF_TIME: str = "9999-12-31"


def _change_hash(tracked_columns: list[str]) -> Column:
    """Build a deterministic hash over the change-tracked columns."""
    parts: list[Column] = [
        sf.coalesce(sf.col(c).cast("string"), sf.lit("\u0000")) for c in tracked_columns
    ]
    return sf.sha2(sf.concat_ws("||", *parts), 256)


def apply_scd2(
    existing: DataFrame | None,
    incoming: DataFrame,
    business_key: str,
    tracked_columns: list[str],
    surrogate_key: str,
    as_of_date: str,
) -> DataFrame:

    stamped: DataFrame = incoming.withColumn("_hash", _change_hash(tracked_columns))

    # ---- First load: every record becomes version 1 ---------------------- #
    if existing is None or existing.limit(1).count() == 0:
        order: WindowSpec = Window.orderBy(business_key)
        return (
            stamped.withColumn(surrogate_key, sf.row_number().over(order))
            .withColumn("dt_valid_from", sf.lit(as_of_date).cast("date"))
            .withColumn("dt_valid_to", sf.lit(END_OF_TIME).cast("date"))
            .withColumn("is_current", sf.lit(True))
        )

    current: DataFrame = existing.filter(sf.col("is_current"))
    history: DataFrame = existing.filter(~sf.col("is_current"))

    cur_hash: DataFrame = current.select(
        sf.col(business_key).alias("_bk"), sf.col("_hash").alias("_cur_hash")
    )
    compared: DataFrame = stamped.join(
        cur_hash, stamped[business_key] == cur_hash["_bk"], "left"
    )

    is_new: Column = sf.col("_cur_hash").isNull()
    is_changed: Column = (~is_new) & (sf.col("_hash") != sf.col("_cur_hash"))

    new_versions_src: DataFrame = compared.filter(is_new | is_changed).select(
        *incoming.columns, "_hash"
    )

    max_sk_row = existing.agg(sf.max(surrogate_key).alias("m")).collect()[0]
    max_sk: int = int(max_sk_row["m"]) if max_sk_row["m"] is not None else 0
    order_new: WindowSpec = Window.orderBy(business_key)
    new_versions: DataFrame = (
        new_versions_src.withColumn(
            surrogate_key, sf.row_number().over(order_new) + sf.lit(max_sk)
        )
        .withColumn("dt_valid_from", sf.lit(as_of_date).cast("date"))
        .withColumn("dt_valid_to", sf.lit(END_OF_TIME).cast("date"))
        .withColumn("is_current", sf.lit(True))
    )

    changed_keys: DataFrame = (
        compared.filter(is_changed)
        .select(sf.col(business_key).alias("_chg_bk"))
        .distinct()
    )
    expired: DataFrame = (
        current.join(
            changed_keys, current[business_key] == changed_keys["_chg_bk"], "inner"
        )
        .drop("_chg_bk")
        .withColumn("dt_valid_to", sf.lit(as_of_date).cast("date"))
        .withColumn("is_current", sf.lit(False))
    )

    unchanged: DataFrame = current.join(
        changed_keys, current[business_key] == changed_keys["_chg_bk"], "left_anti"
    )

    return history.unionByName(unchanged).unionByName(expired).unionByName(new_versions)


def apply_scd1(
    existing: DataFrame | None,
    incoming: DataFrame,
    business_key: str,
    surrogate_key: str,
) -> DataFrame:

    snap: DataFrame = incoming.dropDuplicates([business_key])

    if existing is None or existing.limit(1).count() == 0:
        order: WindowSpec = Window.orderBy(business_key)
        return snap.withColumn(surrogate_key, sf.row_number().over(order))

    existing_keys: DataFrame = existing.select(business_key, surrogate_key)

    # Existing rows whose key is NOT in this batch: retain as-is.
    retained: DataFrame = existing.join(
        snap.select(business_key), on=business_key, how="left_anti"
    )

    # Existing rows whose key IS in this batch: keep surrogate, take new attrs.
    updated: DataFrame = snap.join(existing_keys, on=business_key, how="inner")

    # Brand-new keys: assign surrogate keys above the current maximum.
    max_sk_row = existing.agg(sf.max(surrogate_key).alias("m")).collect()[0]
    max_sk: int = int(max_sk_row["m"]) if max_sk_row["m"] is not None else 0
    new_src: DataFrame = snap.join(
        existing_keys.select(business_key), on=business_key, how="left_anti"
    )
    order_new: WindowSpec = Window.orderBy(business_key)
    minted: DataFrame = new_src.withColumn(
        surrogate_key, sf.row_number().over(order_new) + sf.lit(max_sk)
    )

    return retained.unionByName(updated).unionByName(minted)
