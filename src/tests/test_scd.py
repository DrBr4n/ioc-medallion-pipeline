from __future__ import annotations

from pyspark.sql import DataFrame, Row, SparkSession

from src.transform.scd import apply_scd1, apply_scd2


def _spark() -> SparkSession:
    return (
        SparkSession.builder.master("local[1]")
        .appName("scd-tests")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


def test_scd2_opens_new_version_on_change() -> None:
    spark = _spark()
    spark.sparkContext.setLogLevel("ERROR")

    v1: DataFrame = spark.createDataFrame(
        [Row(athlete_id=1, name="Alicia Vega"), Row(athlete_id=2, name="John Carter")]
    )
    dim1: DataFrame = apply_scd2(None, v1, "athlete_id", ["name"], "athlete_key", "2024-01-01")
    assert dim1.count() == 2, "first load should create one version per key"
    assert dim1.filter("is_current").count() == 2

    v2: DataFrame = spark.createDataFrame(
        [Row(athlete_id=1, name="Alicia Vega-Ruiz"), Row(athlete_id=2, name="John Carter")]
    )
    dim2: DataFrame = apply_scd2(dim1, v2, "athlete_id", ["name"], "athlete_key", "2024-06-01")

    # Athlete 1 changed -> 2 versions; athlete 2 unchanged -> 1 version.
    assert dim2.filter("athlete_id = 1").count() == 2, "changed key keeps history"
    assert dim2.filter("athlete_id = 2").count() == 1, "unchanged key is untouched"
    assert dim2.filter("is_current").count() == 2, "exactly one current row per key"
    current_name = dim2.filter("athlete_id = 1 AND is_current").collect()[0]["name"]
    assert current_name == "Alicia Vega-Ruiz"

    print("OK: SCD2 opens a new version on change and preserves history")
    spark.stop()


def test_scd1_keeps_surrogate_keys_stable() -> None:
    spark = _spark()
    spark.sparkContext.setLogLevel("ERROR")

    v1: DataFrame = spark.createDataFrame(
        [Row(noc="USA", team="United States"), Row(noc="NOR", team="Norway")]
    )
    dim1: DataFrame = apply_scd1(None, v1, "noc", "noc_key")
    usa_key = dim1.filter("noc = 'USA'").collect()[0]["noc_key"]

    # New key 'CZE' sorts before 'NOR'/'USA'; USA's key must NOT be renumbered.
    v2: DataFrame = spark.createDataFrame(
        [Row(noc="CZE", team="Czechia"), Row(noc="USA", team="USA")]
    )
    dim2: DataFrame = apply_scd1(dim1, v2, "noc", "noc_key")

    assert dim2.filter("noc = 'USA'").collect()[0]["noc_key"] == usa_key, (
        "existing surrogate key must stay stable across runs"
    )
    assert dim2.filter("noc = 'USA'").collect()[0]["team"] == "USA", "SCD1 overwrites"
    assert dim2.filter("noc = 'NOR'").count() == 1, "untouched key retained"
    assert dim2.select("noc").distinct().count() == 3, "new key added"

    print("OK: SCD1 preserves surrogate keys and overwrites attributes")
    spark.stop()


if __name__ == "__main__":
    test_scd2_opens_new_version_on_change()
    test_scd1_keeps_surrogate_keys_stable()
