from abc import ABC
from dataclasses import dataclass
from cloudpathlib import CloudPath
from typing import List, Dict, Optional
from bricksync.config import TableFormat, SyncTableType
from enum import Enum
from sqlglot.dialects.dialect import Dialects

@dataclass 
class SyncTableType(Enum):
    VIEW = "view"
    TABLE = "table"
    MATERIALIZED_VIEW = "materialized_view"
    STREAMING_TABLE = "streaming_table"

@dataclass
class TableFormat(Enum):
    DELTA = "delta"
    ICEBERG = "iceberg"
    PARQUET = "parquet"
    AVRO = "avro"
    OTHER = "other"

@dataclass
class Source:
    type: SyncTableType
    table_name: str
    source_dialect: Dialects

@dataclass
class TableSource(Source):
    storage_location: CloudPath
    table_format: TableFormat
    iceberg_metadata_location: CloudPath = None
    partition_keys: List[str] = None

@dataclass
class MaterializedViewSource(Source):
    reconciliation_query: str
    base_tables: List[Source]

@dataclass
class ViewSource(Source):
    view_definition: str
    base_tables: List[Source]

@dataclass
class Target:
    type: SyncTableType
    table_name: str
    exists: bool
    ddl: str
    refresh_query: str

    def set_exists(self, exists: bool):
        self.exists = exists

@dataclass
class ViewTarget(Target):
    base_tables: List[Target]