from __future__ import annotations

import argparse
from datetime import datetime
from typing import Optional

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as sf

from src.transform import bronze, gold, silver
from src import lakehouse
from src.config import PipelineConfig


def run(spark: SparkSession, cfg: PipelineConfig) -> None:
    lakehouse.ensure_namespaces(spark, cfg.namespaces)

    dt_valid_from: str = cfg.effective_date
    year_part: list[Column] = [sf.col("year")]
    ts_part: list[Column] = [sf.days("load_ts")]

    # ---- Bronze (append-only) -------------------------------------------- #
    raw: DataFrame = bronze.ingest_raw(spark, cfg.source_path, cfg.load_ts)
    lakehouse.append(spark, raw, cfg.bronze_athletes, ts_part)

    # ---- Silver (clean current batch, upsert on grain) ------------------- #
    silver_batch: DataFrame = silver.clean(raw)
    lakehouse.upsert(spark, silver_batch, cfg.silver_athletes, "participation_id", year_part)

    # ---- Regions reference (optional second source) ---------------------- #
    if cfg.regions_path is not None:
        regions_raw: DataFrame = bronze.ingest_regions(spark, cfg.regions_path, cfg.load_ts)
        lakehouse.append(spark, regions_raw, cfg.bronze_regions, ts_part)
        regions_clean: DataFrame = silver.clean_regions(regions_raw)
        lakehouse.upsert(spark, regions_clean, cfg.silver_regions, "noc", [])
    regions_full: Optional[DataFrame] = lakehouse.read_or_none(spark, cfg.silver_regions)

    # ---- Gold dimensions (batch snapshot merged into existing) ----------- #
    dim_athlete: DataFrame = gold.build_dim_athlete(
        silver_batch, lakehouse.read_or_none(spark, cfg.dim("dim_athlete")), dt_valid_from
    )
    dim_games: DataFrame = gold.build_dim_games(
        silver_batch, lakehouse.read_or_none(spark, cfg.dim("dim_games"))
    )
    dim_event: DataFrame = gold.build_dim_event(
        silver_batch, lakehouse.read_or_none(spark, cfg.dim("dim_event"))
    )
    dim_noc: DataFrame = gold.build_dim_noc(
        silver_batch, regions_full, lakehouse.read_or_none(spark, cfg.dim("dim_noc"))
    )

    dim_athlete.cache()
    dim_games.cache()
    dim_event.cache()
    dim_noc.cache()

    lakehouse.overwrite_dim(dim_athlete, cfg.dim("dim_athlete"))
    lakehouse.overwrite_dim(dim_games, cfg.dim("dim_games"))
    lakehouse.overwrite_dim(dim_event, cfg.dim("dim_event"))
    lakehouse.overwrite_dim(dim_noc, cfg.dim("dim_noc"))

    # ---- Gold fact (upsert on participation_id into year partitions) ----- #
    fact_batch: DataFrame = gold.build_fact(
        silver_batch, dim_athlete, dim_games, dim_event, dim_noc
    )
    lakehouse.upsert(spark, fact_batch, cfg.fact, "participation_id", year_part)

    print(f"Pipeline completed for load_ts={cfg.load_ts}")


def main() -> None:
    parser = argparse.ArgumentParser(description="IOC Olympic Iceberg pipeline")
    parser.add_argument("--source", required=True, help="Path to the raw CSV batch")
    parser.add_argument("--lake", required=True, help="Lake root (warehouse + catalog)")
    parser.add_argument(
        "--regions", required=False, help="Path to the NOC->region reference CSV"
    )
    parser.add_argument(
        "--dt_valid_from", required=False, help="Business dt_valid_from date YYYY-MM-DD for SCD2 (default: ingestion date)"
    )
    args = parser.parse_args()

    load_ts: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    cfg = PipelineConfig(
        source_path=args.source,
        lake_root=args.lake,
        load_ts=load_ts,
        regions_path=args.regions,
        dt_valid_from=args.dt_valid_from
    )
    spark = lakehouse.get_spark(cfg)
    try:
        run(spark, cfg)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
