from pydantic.dataclasses import dataclass
from pydantic import Field
from typing import Optional, Dict, List
import yaml, dataclasses
from pathlib import Path
from enum import Enum
from databricks.sdk.service.catalog import TableType

class TargetSyncStrategy(Enum):
    MIRROR = "mirror" # Mirrors the catalog, schema, and table structure from the source to the target
    CUSTOM_SCHEMA = "custom_schema" # Mirrors the catalog and tablename but with custom schema
    CUSTOM_CATALOG = "custom_catalog" # Mirrors the tablename and schema but with custom catalog

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

class ProviderType(Enum):
    DATABRICKS = "databricks"
    SNOWFLAKE = "snowflake"
    REDSHIFT = "redshift"
    BIGQUERY = "bigquery"
    AZURE = "azure"
    AWS = "aws"
    GLUE = "glue"
    ATHENA = "athena"


@dataclass
class ProviderConfig:
    provider: ProviderType
    configuration: Optional[Dict[str, str]] = Field(default_factory=dict)

@dataclass
class SyncConfig:
    source: str
    source_provider: str
    target_provider: str
    configuration: Optional[Dict[str, str]] = None

@dataclass
class SyncOptions:
    target_catalog_ovveride: Optional[str] = None
    target_schema_override: Optional[str] = None
    target_table_override: Optional[str] = None
    base_table_catalog_override: Optional[str] = None
    base_table_schema_override: Optional[str] = None
    target_provider_options: Optional[Dict[str, str]] = None 

@dataclass
class BrickSyncConfig:
    providers: List[Dict[str, ProviderConfig]]
    syncs: List[SyncConfig] = dataclasses.field(default_factory=list) 
    skip_failures: bool = False
    continuous: bool = False
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
        if not provider.configuration:
            provider.configuration = {}
        self.providers.append({name: provider})
    def get_provider_config(self, name: str):
        for provider in self.providers:
            if name in provider.keys():
                return provider[name]