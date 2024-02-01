from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import TableInfo, TableType
from bricksync.provider.source import SourceProvider
from bricksync.config import ProviderConfig, SyncConfig, SyncTableType, TableFormatPreference
from bricksync.table import Source, TableSource, ViewSource, TableFormat
from cloudpathlib import CloudPath
from typing import List
from dataclasses import dataclass
import logging
from sqlglot.dialects.dialect import Dialects

@dataclass
class UniformIcebergInfo:
    metadata_location: CloudPath
    converted_delta_version: int
    converted_delta_timestamp: str

class DatabricksSource(SourceProvider):
    def __init__(self, provider_config: ProviderConfig, target_format_preference: TableFormatPreference):
        self.provider_config = provider_config
        self.client = self.authenticate()
        self.target_format_preference = target_format_preference

    @classmethod
    def initialize(cls, provider_config: ProviderConfig, target_format_preference: TableFormatPreference):
        return cls(provider_config, target_format_preference)
    
    def authenticate(self):
        try:
          client = (WorkspaceClient() if not self.provider_config.configuration else
              WorkspaceClient(
              host=self.provider_config.configuration['host'] if 'host' in self.provider_config.configuration else None,
              token=self.provider_config.token if self.provider_config.token else None,
              profile= self.provider_config.configuration['profile'] if 'profile' in self.provider_config.configuration else None)
          )
          client.current_user.me()
          return client
        except: 
          raise

    def _get_partition_columns(self, table_info: TableInfo) -> List[str]:
        partition_cols = []
        for col in table_info.columns:
            if col.partition_index:
                partition_cols.insert(col.partition_index, col.name)
        return partition_cols if len(partition_cols) > 0 else None


    def _get_latest_iceberg_metadata(self, table_name: str) -> UniformIcebergInfo:
        """Workaround for sdk TableInfo dataclass not including this info. Eventually we can get rid of this second call"""
        extended = self.client.api_client.do("GET", f"/api/2.1/unity-catalog/tables/{table_name}")
        if "delta_uniform_iceberg" not in extended:
            return None
        
        extended = extended["delta_uniform_iceberg"]

        return UniformIcebergInfo(
            metadata_location=CloudPath(extended["metadata_location"]),
            converted_delta_version=extended["converted_delta_version"],
            converted_delta_timestamp=extended["converted_delta_timestamp"]
        )
    
    
    def _get_target_format_with_preference(self, source_format: TableFormat, iceberg_metadata: str = None) -> TableFormat:
        if (source_format.value == TableFormat.DELTA.value
            and (
                self.target_format_preference.value == TableFormatPreference.ICEBERG_PREFERRED.value
                or self.target_format_preference.value == TableFormatPreference.ICEBERG_ONLY.value
                )
            and iceberg_metadata):
            return TableFormat.ICEBERG
        else:
            return source_format
        
    def _get_source_format(self, table_info: TableInfo) -> TableFormat:
       if table_info.data_source_format.value == 'DELTA':
            return TableFormat.DELTA
       elif table_info.data_source_format.value == 'ICEBERG':
           return TableFormat.ICEBERG
       elif table_info.data_source_format.value == 'PARQUET':
           return TableFormat.PARQUET
       elif table_info.data_source_format.value == 'AVRO':
            return TableFormat.AVRO
       else:
           return TableFormat.OTHER
       
    def _get_table_source(self, table_info: TableInfo, table_type: SyncTableType) -> TableSource:
         partition_keys = self._get_partition_columns(table_info)
         iceberg_metadata = self._get_latest_iceberg_metadata(table_info.full_name)
         table_format = self._get_target_format_with_preference(self._get_source_format(table_info), iceberg_metadata)
         if table_format.value != TableFormat.ICEBERG.value:
             logging.warn(f"Skipping {table_info.full_name}: Only iceberg tables are supported for now")
             return None
         
         return TableSource(
             type=table_type,
             table_name=table_info.full_name,
             source_dialect=Dialects.DATABRICKS,
             storage_location=CloudPath(table_info.storage_location),
             table_format=table_format,
             iceberg_metadata_location=iceberg_metadata.metadata_location if iceberg_metadata else None,
             partition_keys=partition_keys
         )
         
    
    def _get_view_source(self, table_info: TableInfo) -> ViewSource:
        view_query = table_info.view_definition.replace('`','')
        view_dependencies = [vd.table.table_full_name for vd in table_info.view_dependencies.dependencies]
        base_table_names = [bt for bt in view_dependencies if bt in view_query]
        base_tables = [self._get_source(bt) for bt in base_table_names]
        if None in base_tables:
            logging.warn(f"Skipping view {table_info.full_name}: One or more base tables don't support Iceberg")
            return None
        return ViewSource(
            type=SyncTableType.VIEW,
            table_name=table_info.full_name,
            source_dialect=Dialects.DATABRICKS,
            view_definition=view_query,
            base_tables=base_tables
        )
    
    def _get_mv_source(self, table_info: TableInfo) -> ViewSource:
        # Find reconciliation query
        reconciliation_query = table_info.properties["spark.internal.materialized_view.reconciliation_query"].replace('`','')
        # Find all base tables
        view_dependencies = [vd.table.table_full_name for vd in table_info.view_dependencies.dependencies]
        base_table_names = [bt for bt in view_dependencies if bt in reconciliation_query]
        base_tables = [self._get_source(bt) for bt in base_table_names]
        if None in base_tables:
            logging.warn(f"Skipping materialized view {table_info.full_name}: One or more base tables don't support Iceberg")
            return None
        return ViewSource(
            type=SyncTableType.VIEW,
            table_name=table_info.full_name,
            source_dialect=Dialects.DATABRICKS,
            view_definition=reconciliation_query,
            base_tables=base_tables
        )
    
    def _unpack_sources(self, sync_config: SyncConfig) -> List[str]:
        """Unpacks a source string into a list of table sources if it is a schema or catalog"""
        source_parts = sync_config.source.split('.')
        if len(source_parts) == 3:
            return [sync_config.source]
        elif len(source_parts) == 2:
            tables = self.client.tables.list(source_parts[0], source_parts[1])
            sources = [table.full_name for table in tables]
            return sources
        else:
            sources = []
            schemas = self.client.schemas.list(source_parts[0])
            for schema in schemas:
                # Skip information_schema
                if str.lower(schema.name) == "information_schema":
                    continue
                tables = self.client.tables.list(source_parts[0], schema.name)
                sources += [table.full_name for table in tables]
            return sources
        
    def _get_source(self, table_name: str) -> Source:
        try:
          table = self.client.tables.get(table_name, include_delta_metadata=True)
        except Exception as e:
            raise Exception(f"Error getting table {table_name}: {e}")
        
        table_type = SyncTableType.from_tabletype(table.table_type)
        
        if table_type == SyncTableType.TABLE:
            table_source = self._get_table_source(table, table_type)
            # Only tables with supported storage fmts will be returned
            if table_source:
                return table_source
        elif table_type == SyncTableType.MATERIALIZED_VIEW:
            mv_source = self._get_mv_source(table)
            return mv_source
        elif table_type == SyncTableType.VIEW:
            view_source = self._get_view_source(table)
            return view_source
        else:
            logging.info(f"Skipping source {table_name}: Databricks table types of {table_type} are not supported yet")
    
    def get_sources(self, sync_config: SyncConfig) -> List[Source]:
        table_sources = self._unpack_sources(sync_config)
        sources = []
        for source in table_sources:
          source_obj = self._get_source(source)
          if source_obj:
            sources.append(source_obj)
        return sources
        