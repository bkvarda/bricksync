from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import TableInfo, TableType, DataSourceFormat
from sqlglot import Dialect
from bricksync.provider.catalog import CatalogProvider
from bricksync.provider.databricks import DatabricksProvider
from bricksync.config import ProviderConfig
from bricksync.table import Table, DeltaTable, IcebergTable, View, UniformIcebergInfo
from typing import List, Union
from databricks.sql.client import Connection
import logging, time
import sqlglot
from sqlglot.dialects.dialect import Dialects

class DatabricksCatalog(CatalogProvider):
    def __init__(self, provider: DatabricksProvider):
        self.provider = provider
        self.client: WorkspaceClient = provider.client
        self.sql_client: Connection = provider.sql_client

    @classmethod
    def initialize(cls, provider_config: ProviderConfig):
        return cls(provider=DatabricksProvider.initialize(provider_config))

    def _get_table_internal(self, table_name: str) -> TableInfo:
        return self.client.tables.get(table_name)
    
    def get_uniform_iceberg_metadata(self, table_name: str) -> UniformIcebergInfo:
        """Workaround for sdk TableInfo dataclass not including this info. Eventually we can get rid of this second call"""
        extended = self.client.api_client.do("GET", f"/api/2.1/unity-catalog/tables/{table_name}")
        if "delta_uniform_iceberg" not in extended:
            return None
        
        extended = extended["delta_uniform_iceberg"]

        return UniformIcebergInfo(
            metadata_location=extended["metadata_location"],
            converted_delta_version=extended["converted_delta_version"],
            converted_delta_timestamp=extended["converted_delta_timestamp"]
        )
     
    def create_catalog(self, catalog_name: str):
        try:
           self.client.catalogs.create(catalog_name)
           return
        except Exception as e:
            if 'already exists' in str(e):
              return
            else:
              raise(e)
    
    def create_schema(self, catalog_name: str, schema_name: str):
        try:
            self.client.schemas.create(schema_name, catalog_name=catalog_name)
            return
        except Exception as e:
            if 'already exists' in str(e):
              return
            else:
              raise(e)
    
    def sql(self, statement: str):
        return self.sql_client.cursor().execute(statement).fetchall_arrow()
    
    def get_uniform_iceberg_metadata(self, table_name: str) -> UniformIcebergInfo:
        """Workaround for sdk TableInfo dataclass not including this info. Eventually we can get rid of this second call"""
        extended = self.client.api_client.do("GET", f"/api/2.1/unity-catalog/tables/{table_name}")
        if "delta_uniform_iceberg" not in extended:
            return None
        
        extended = extended["delta_uniform_iceberg"]

        return UniformIcebergInfo(
            metadata_location=extended["metadata_location"],
            converted_delta_version=extended["converted_delta_version"],
            converted_delta_timestamp=extended["converted_delta_timestamp"]
        )
    
    def generate_iceberg_metadata(self, table_name: str, timeout_seconds: int = 300) -> DeltaTable:
        """Synchronously generate Iceberg metadata for a table by comparing latest Delta version to 
        most current UniForm version and ensuring they are equivalent before exiting."""
        logging.info(f"Generating Iceberg metadata for table {table_name}")
        # Get table
        tbl : DeltaTable = self.get_table(table_name)
        properties = tbl.delta_properties

        if not properties.get("delta.enableIcebergCompatV2") == "true":
            raise Exception(f"Table {table_name} is not a UniForm table")
        
        # Get latest delta version
        detail = self.sql(f"DESCRIBE HISTORY {table_name} LIMIT 1")
        last_delta_version = detail.column("version").to_pylist()[0]
        last_uniform_version = (tbl.uniform_iceberg_info.converted_delta_version 
                                if tbl.uniform_iceberg_info else 0)
        logging.info(f"Last delta version: {last_delta_version}. Last uniform version: {last_uniform_version}")
        
        if last_delta_version <= last_uniform_version:
            logging.info(f"Table {table_name} Iceberg metadata is already up to date")
            return tbl
        # Gen metadata
        self.sql(f"MSCK REPAIR TABLE {table_name} SYNC METADATA")
        # Wait for Uniform delta version to match or > than delta version
        backoff = 1
        timeout_start = time.time()
        while time.time() < timeout_start + timeout_seconds:
            backoff *=2
            tbl : DeltaTable = self.get_table(table_name)
            current_uniform_version = (tbl.uniform_iceberg_info.converted_delta_version if tbl.uniform_iceberg_info else 0)
            logging.info(f"Last delta version: {last_delta_version}. Current uniform version: {current_uniform_version}")
            if current_uniform_version >= last_delta_version:
                logging.info(f"Table {table_name} Iceberg metadata updated to version {current_uniform_version} from {last_uniform_version}")
                return tbl
            time.sleep(backoff)
        raise Exception(f"Timed out waiting for Iceberg metadata to be generated for table {table_name}")
            
    def get_table(self, table_name: str) -> Union[View, Table]:
        table_info = self.client.tables.get(table_name, include_delta_metadata=True)
        if table_info.table_type in [TableType.MANAGED,TableType.EXTERNAL]:
            if table_info.data_source_format != DataSourceFormat.DELTA:
                raise Exception(f"Table {table_name} is not a Delta table. Only Delta tables are supported currently.")
            iceberg_metadata = self.get_uniform_iceberg_metadata(table_name)
            return DeltaTable(
                name=table_name,
                storage_location=table_info.storage_location,
                uniform_iceberg_info=iceberg_metadata,
                delta_properties=table_info.properties
            )
        elif table_info.table_type in [TableType.VIEW, TableType.STREAMING_TABLE, TableType.MATERIALIZED_VIEW] :
            view_def = (table_info.view_definition if 
                        table_info.table_type in [TableType.VIEW] 
                        else None)
            # Handling for MV and ST
            if not view_def:
                properties = table_info.properties
                print(table_info.table_type.value)
                view_def = properties.get(f"spark.internal.{table_info.table_type.value.lower()}.reconciliation_query", None)
            view_query = view_def.replace('`','')
            view_dependencies = [vd.table.table_full_name for vd in table_info.view_dependencies.dependencies]
            base_table_names = [bt for bt in view_dependencies if bt in view_query]
            base_tables = [self.get_table(bt) for bt in base_table_names]
            return View(
                name=table_name,
                view_definition=view_query,
                dialect=Dialects.DATABRICKS,
                base_tables=base_tables
            )
        else:
            raise Exception(f"Table type {table_info.table_type.value} is not currently supported")

    
    def create_or_refresh_external_table(self, table: Union[DeltaTable, IcebergTable]):
        # Given an Iceberg table, convert to Delta
        if table.is_delta():
            statement = f"""CREATE TABLE IF NOT EXISTS {table.name}
            USING DELTA
            LOCATION '{table.storage_location}'
            """ 
            self.sql(statement)
        elif table.is_iceberg():
            statement = f"""CREATE TABLE IF NOT EXISTS {table.name}
            UNIFORM iceberg
            METADATA_PATH '{table.iceberg_metadata_location}';
            REFRESH TABLE {table.name} METADATA_PATH '{table.iceberg_metadata_location}';
            """ 
            self.sql(statement)
        else:
            raise Exception(f"Unsupported table type for table {table.name}")
    
    def create_or_refresh_view(self, view: View):
        # Issue query to create or refresh view
        converted_view_def = self.convert_view_dialect(view.view_definition, view.dialect)
        statement = f"""CREATE OR REPLACE VIEW {view.name} AS {converted_view_def}"""
        return None
    
    def convert_view_dialect(self, view_definition: str, source_dialect: Dialect):
        try:
          converted = sqlglot.transpile(view_definition, read = source_dialect, write = Dialects.DATABRICKS)
        except Exception as e:
            raise Exception(f"Error converting view definition from {source_dialect} to Databricks dialect: {e}")
        return converted[0]

        