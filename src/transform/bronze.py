from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as sf

def read_csv(spark: SparkSession, source_path: str, load_ts: str) -> DataFrame:
    raw: DataFrame = (
        spark.read.option("header", True)
        .csv(source_path)
    )
    return raw.withColumn("load_ts", sf.lit(load_ts).cast("timestamp"))

def ingest_raw(spark: SparkSession, source_path: str, load_ts: str) -> DataFrame:
    return read_csv(spark, source_path, load_ts)


def ingest_regions(spark: SparkSession, regions_path: str, load_ts: str) -> DataFrame:
    return read_csv(spark, regions_path, load_ts)
