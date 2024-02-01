from pydantic.dataclasses import dataclass
from typing import Optional, Dict, List
import yaml, dataclasses
from pathlib import Path
from enum import Enum
from databricks.sdk.service.catalog import TableType

class TargetSyncStrategy(Enum):
    MIRROR = "mirror"

class TableFormatPreference(Enum):
    DELTA = "delta"
    ICEBERG = "iceberg"
    DELTA_PREFERRED = "delta_preferred"
    ICEBERG_PREFERRED = "iceberg_preferred"

class TableFormat(Enum):
    DELTA = "delta"
    ICEBERG = "iceberg"
    AVRO = "avro"
    PARQUET = "parquet"

class SyncTableType(Enum):
    VIEW = "view"
    TABLE = "table"
    MATERIALIZED_VIEW = "materialized_view"
    STREAMING_TABLE = "streaming_table"

    def from_tabletype(table_type: TableType):
        if table_type == TableType.VIEW:
            return SyncTableType.VIEW
        elif table_type == TableType.EXTERNAL or table_type == TableType.MANAGED:
            return SyncTableType.TABLE
        elif table_type == TableType.MATERIALIZED_VIEW:
            return SyncTableType.MATERIALIZED_VIEW
        elif table_type == TableType.STREAMING_TABLE:
            return SyncTableType.STREAMING_TABLE
        else:
            raise Exception("Unknown table type")

class ProviderType(Enum):
    DATABRICKS = "databricks"
    SNOWFLAKE = "snowflake"
    REDSHIFT = "redshift"
    BIGQUERY = "bigquery"
    AZURE = "azure"
    AWS = "aws"
    GLUE = "glue"
    ATHENA = "athena"

class ProviderConfigType(Enum):
    SOURCE = "source"
    TARGET = "target"

@dataclass
class ProviderConfig:
    provider: ProviderType
    type: ProviderConfigType
    username: Optional[str] = None
    password: Optional[str] = None
    token: Optional[str] = None
    private_key_file: Optional[str] = None
    private_key_str: Optional[str] = None
    host: Optional[str] = None
    configuration: Optional[Dict[str, str]] = None

@dataclass
class SyncConfig:
    source: str
    source_provider: str
    target_provider: str
    source_configuration: Optional[Dict[str, str]] = None

@dataclass
class BrickSyncConfig:
    providers: List[Dict[str, ProviderConfig]]
    syncs: List[SyncConfig] = dataclasses.field(default_factory=list) 
    skip_failures: bool = False
    continuous: bool = False
    target_format_preference: TableFormatPreference = TableFormatPreference.DELTA_PREFERRED
    target_sync_strategy: TargetSyncStrategy = TargetSyncStrategy.MIRROR
    default_source_provider: Optional[str] = None
    default_target_provider: Optional[str] = None
    secret_provider: Optional[str] = None
    @classmethod
    def load(cls, config_path):
        yml = yaml.safe_load(Path(config_path).read_text())
        return cls(**yml)
    @classmethod
    def new(cls):
        return cls([], [])
    def add_sync(self, sync: SyncConfig):
        self.syncs.append(sync)
    def add_provider(self, name: str, provider: ProviderConfig):
        self.providers.append({name: provider})