from __future__ import annotations
 
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
 
CATALOG: str = "ioc"
 
 
@dataclass(frozen=True)
class PipelineConfig:
    """Immutable run configuration.
 
    Attributes:
        source_path:    Raw athlete_events CSV batch to ingest.
        lake_root:      Root dir holding the Iceberg warehouse + SQLite catalog.
        load_ts:        Logical load timestamp, set in the driver
                        at run start. Drives the latest-wins upsert tiebreaker.
        regions_path:   Optional NOC_regions CSV
        dt_valid_from:  Optional business "dt_valid_from" date for SCD2. Defaults
                        to the ingestion date when omitted; pass it explicitly
                        for backfills of historical data.
    """
 
    source_path: str
    lake_root: str
    load_ts: str
    regions_path: Optional[str] = None
    dt_valid_from: Optional[str] = None
 
    @property
    def effective_date(self) -> str:
        """Business date used for SCD2 ranges: ``dt_valid_from`` or the load date."""
        return self.dt_valid_from if self.dt_valid_from is not None else self.load_ts.split(" ")[0]
 
    # ---- Iceberg catalog wiring ------------------------------------------ #
    @property
    def warehouse_uri(self) -> str:
        """File URI of the Iceberg warehouse (where data/metadata files live)."""
        return Path(self.lake_root).resolve().joinpath("warehouse").as_uri()
 
    @property
    def catalog_uri(self) -> str:
        """JDBC URI of the SQLite catalog DB (the table-pointer store)."""
        db = Path(self.lake_root).resolve().joinpath("catalog.db")
        return f"jdbc:sqlite:{db}"
 
    # ---- Table identifiers ----------------------------------------------- #
    @property
    def bronze_athletes(self) -> str:
        return f"{CATALOG}.bronze.olympic_results"
 
    @property
    def bronze_regions(self) -> str:
        return f"{CATALOG}.bronze.noc_regions"
 
    @property
    def silver_athletes(self) -> str:
        return f"{CATALOG}.silver.olympic_results"
 
    @property
    def silver_regions(self) -> str:
        return f"{CATALOG}.silver.noc_regions"
 
    def dim(self, name: str) -> str:
        return f"{CATALOG}.gold.{name}"
 
    @property
    def fact(self) -> str:
        return f"{CATALOG}.gold.fact_olympic_result"
 
    @property
    def namespaces(self) -> tuple[str, ...]:
        return (f"{CATALOG}.bronze", f"{CATALOG}.silver", f"{CATALOG}.gold")