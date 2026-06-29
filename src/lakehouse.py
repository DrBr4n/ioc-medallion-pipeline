from __future__ import annotations

import os
from typing import Optional

from pyspark.sql import Column, DataFrame, SparkSession

from src.config import CATALOG, PipelineConfig


def get_spark(cfg: PipelineConfig) -> SparkSession:
    os.makedirs(cfg.lake_root, exist_ok=True)
    return (
        SparkSession.builder.appName("ioc-olympic-lakehouse")
        .master("local[*]")
        .config(
            "spark.jars.packages", (
                "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,"
                "org.xerial:sqlite-jdbc:3.46.1.0"
            )
        )
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{CATALOG}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(f"spark.sql.catalog.{CATALOG}.type", "jdbc")
        .config(f"spark.sql.catalog.{CATALOG}.uri", cfg.catalog_uri)
        .config(f"spark.sql.catalog.{CATALOG}.warehouse", cfg.warehouse_uri)
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def ensure_namespaces(spark: SparkSession, namespaces: tuple[str, ...]) -> None:
    for ns in namespaces:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")


def table_exists(spark: SparkSession, table: str) -> bool:
    try:
        return bool(spark.catalog.tableExists(table))
    except Exception:
        return False


def read_or_none(spark: SparkSession, table: str) -> Optional[DataFrame]:
    return spark.table(table) if table_exists(spark, table) else None


def append(
    spark: SparkSession,
    df: DataFrame,
    table: str,
    partition: list[Column]
) -> None:
    if table_exists(spark, table):
        df.writeTo(table).append()
    else:
        writer = df.writeTo(table).using("iceberg")
        if partition:
            writer.partitionedBy(*partition)
        writer.create()


def upsert(
    spark: SparkSession,
    df: DataFrame,
    table: str,
    key: str,
    partition: list[Column],
) -> None:
    if not table_exists(spark, table):
        writer = df.writeTo(table).using("iceberg")
        if partition:
            writer.partitionedBy(*partition)
        writer.create()
        return
    df.createOrReplaceTempView("_stage")
    spark.sql(
        f"""
        MERGE INTO {table} t
        USING _stage s
        ON t.{key} = s.{key}
        WHEN MATCHED AND s.load_ts >= t.load_ts THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )


def overwrite_dim(df: DataFrame, table: str) -> None:
    df.writeTo(table).using("iceberg").createOrReplace()
