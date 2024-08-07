from bricksync.provider.snowflake import SnowflakeProvider
from bricksync.provider.catalog import CatalogProvider
from bricksync.config import ProviderConfig
from typing import List, Union, Optional, Dict
from bricksync.table import Table, DeltaTable, IcebergTable, View
from snowflake.connector.cursor import DictCursor, SnowflakeCursor
from snowflake.connector import SnowflakeConnection
import snowflake.connector as sf
from dataclasses import dataclass
import json, logging
import sqlglot
import sqlglot.expressions as exp
from sqlglot.dialects.dialect import Dialect, Dialects
from enum import Enum


class SnowflakeTableType(Enum):
    VIEW = "VIEW"
    TABLE = "TABLE"

@dataclass 
class SnowflakeCatalogIntegration:
    name: str
    catalog_source: str
    table_format: str
    enabled: bool
    refresh_interval_seconds: int = None
    comment: str = None

@dataclass
class SnowflakeExternalVolume:
    name: str
    storage_provider: str
    storage_base_url: str

    def to_iceberg_metadata_string(self, full_iceberg_metadata_path: str):
        if ".json" not in full_iceberg_metadata_path:
            raise Exception(f"Metadata file must be a JSON file. Got {full_iceberg_metadata_path}")
        metadata_str = full_iceberg_metadata_path.split(self.storage_base_url)[1]
        if len(metadata_str) == 0:
            raise Exception(f"Metadata path must be a subdirectory of the base URL: {full_iceberg_metadata_path}")
        if metadata_str[0] == '/':
            metadata_str = metadata_str[1:]
        return metadata_str
    
    def to_delta_metadata_string(self, delta_table_path: str):
        metadata_str = delta_table_path.split(self.storage_base_url)[1]
        if len(metadata_str) == 0:
            return '/'
        if metadata_str[0] == '/':
            metadata_str = metadata_str[1:]
        return metadata_str

class SnowflakeCatalog(CatalogProvider):
    def __init__(self, provider: SnowflakeProvider):
        self.provider = provider
        self.client: SnowflakeConnection = provider.client

    def _sql(self, sql: str):
        return self.client.cursor(DictCursor).execute(sql)
    
    def _format_describe_response(self, response):
        return {str.lower(rec['property']): rec['property_value'] for rec in response.fetchall()}
    
    def _print_full_response(self, response: SnowflakeCursor):
        for rec in response.fetchall():
            print(rec)

    def _get_iceberg_metadata_location(self, table_name: str) -> str:
        try:
           q = self._sql(f"SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION('{table_name}') as ICEBERG_INFO")
           iceberg_info_str = q.fetchone()['ICEBERG_INFO']
           print(iceberg_info_str)
           iceberg_info = json.loads(iceberg_info_str)
           iceberg_metadata = iceberg_info["metadataLocation"]
           return iceberg_metadata
        except Exception as e:
            raise Exception(f"Error getting Iceberg metadata location for table {table_name}: {e}")

    @classmethod
    def initialize(cls, provider_config: ProviderConfig):
        return cls(provider=SnowflakeProvider.initialize(provider_config))

    def create_catalog_integration(self, name: str, source: str = 'OBJECT_STORE', table_format: str = 'ICEBERG', enabled: bool = True):
        self._sql(f"""CREATE CATALOG INTEGRATION IF NOT EXISTS {name} 
                  CATALOG_SOURCE = {source} 
                  TABLE_FORMAT = {table_format}
                  ENABLED = {enabled}""")
    
    def get_catalog_integration(self, name: str = None, table_format: str = "ICEBERG") -> SnowflakeCatalogIntegration:
        """Get a Snowflake catalog integration by name. If no name is specified, attempts to get one"""
        if not name:
            integrations = self.list_catalog_integrations()
            for integration in integrations:
                if (str.upper(integration.table_format) == str.upper(table_format)
                    and str.upper(integration.catalog_source) == 'OBJECT_STORE' 
                    and integration.enabled):
                    return integration
            raise Exception(f"No {table_format} catalog integration found. Create one.")
        else:
            res = self._format_describe_response(
            self._sql(f"DESCRIBE CATALOG INTEGRATION {name}"))
            print(res)
            return SnowflakeCatalogIntegration(
                name=name,
                **res)
    
    def list_catalog_integrations(self) -> List[SnowflakeCatalogIntegration]:
        q = self._sql(f"SHOW CATALOG INTEGRATIONS")
        integrations = []
        for integration in q.fetchall():
            integrations.append(self.get_catalog_integration(integration['name']))
        return integrations
    
    def get_catalog(self, name: str):
        q = self._sql(f"DESCRIBE DATABASE {name}")

        return self._format_describe_response(q)
    
    def create_catalog(self, catalog_name: str):
        return self._sql(f"""CREATE DATABASE IF NOT EXISTS {catalog_name}""")
    
    def create_schema(self, catalog_name: str, schema_name: str):
        return self._sql(f"""CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}""")
    
    def get_object_type(self, object_name: str) -> SnowflakeTableType:
        try:
            q = self._sql(f"SELECT SYSTEM$REFERENCE('VIEW', '{object_name}')")
            print(q.fetchall())
            return SnowflakeTableType.VIEW
        except:
            try:
                q = self._sql(f"SELECT SYSTEM$REFERENCE('TABLE', '{object_name}')")
                return SnowflakeTableType.TABLE
            except:
                raise Exception(f"Object {object_name} not found as table or view")
                   
    def get_table(self, table_name: str) -> Union[IcebergTable, DeltaTable, View]:
        object_type = self.get_object_type(table_name)
        if object_type == SnowflakeTableType.VIEW:
            q = self._sql(f"SELECT GET_DDL('VIEW','{table_name}', true) as VIEW_DDL")
            ddl_str = q.fetchone()['VIEW_DDL']
            expression = sqlglot.parse_one(ddl_str, read=Dialects.SNOWFLAKE)
            base_table_list = list(expression.find_all(exp.Table))
            base_table_list_fmt = [f"{bt.catalog}.{bt.db}.{bt.name}" for bt in base_table_list]
            base_tables = [self.get_table(bt) 
                           for bt in base_table_list_fmt if bt not in [table_name.upper(), table_name.lower()]]
            return View(name=table_name,
                        view_definition=ddl_str,
                        dialect=Dialects.SNOWFLAKE,
                        base_tables=base_tables)
        else:
            iceberg_metadata = self._get_iceberg_metadata_location(table_name)
            return IcebergTable(name=table_name,
                                storage_location=iceberg_metadata.split('/metadata')[0],
                                iceberg_metadata_location=iceberg_metadata)
     
    def create_external_table(self, table: Union[IcebergTable, DeltaTable], replace=False, **kwargs):
        if not table.is_iceberg():
            raise Exception(f"Table {table.name} does not have Iceberg metadata")
        table_name = table.name
        location = table.iceberg_metadata_location
        table_format = "ICEBERG"
        catalog_integration = self.get_catalog_integration(table_format=table_format)
        external_volume = self.get_external_volume_by_path(location)
        metadata_str = (
            f"METADATA_FILE_PATH='{external_volume.to_iceberg_metadata_string(location)}'"
            if str.upper(table_format) == 'ICEBERG' else
            f"BASE_LOCATION='{location}'"
        )
        statement = (
            f"""CREATE ICEBERG TABLE IF NOT EXISTS {table_name} 
            EXTERNAL_VOLUME='{external_volume.name}'
            CATALOG='{catalog_integration.name}'
            {metadata_str}""")
        
        if replace:
            statement = (
            f"""CREATE OR REPLACE ICEBERG TABLE {table_name} 
            EXTERNAL_VOLUME='{external_volume.name}'
            CATALOG='{catalog_integration.name}'
            {metadata_str}
            COPY GRANTS""")

        logging.info(f"Creating external table {table_name} with statement: {statement}")
        return self._sql(statement)
    
    def refresh_external_table(self, table: Union[IcebergTable, DeltaTable], **kwargs):
        if not table.is_iceberg():
            raise Exception(f"Table {table.name} does not have Iceberg metadata")
        table_name = table.name
        location = table.iceberg_metadata_location
        table_format = "ICEBERG"
        external_volume = self.get_external_volume_by_path(location)
        metadata_file_path = external_volume.to_iceberg_metadata_string(location)
        statement =  f"""ALTER ICEBERG TABLE {table_name} REFRESH '{metadata_file_path}'"""
        try:
           result = self._sql(statement)
           return result
        except Exception as e:
            logging.info(f"Snowflake error on refresh attempt: {str(e)}")
            if "does not match the table uuid in metadata file" in str(e).lower():
                logging.info(f"Table UUID does not match - source was likely overwritten - attempting to recreate table")
                self.create_external_table(table, replace=True, **kwargs)
                return self.refresh_external_table(table, **kwargs)
            else:
                raise

    
    def create_or_refresh_external_table(self, table: Union[IcebergTable, DeltaTable], **kwargs):
        # If table exists
        if not table.is_iceberg():
            raise Exception(f"Table {table.name} does not have Iceberg metadata")
        table_name = table.name
        location = table.iceberg_metadata_location
        table_format = "ICEBERG"
        try:
          existing_table = self.get_table(table_name)
          return self.refresh_external_table(table)
        #todo: handle specific exception
        except:
            self.create_external_table(table)
            return self.refresh_external_table(table)
    
    def create_or_refresh_view(self, view: View, **kwargs):
        name = view.name
        view_def = self.convert_view_dialect(view.view_definition, view.dialect)
        q = self._sql(f"""CREATE OR REPLACE VIEW {name} 
                      COPY GRANTS AS {view_def}""")
        return q
    
    def convert_view_dialect(self, view_definition: str, source_dialect: Dialect):
        try:
          converted = sqlglot.transpile(view_definition, read = source_dialect, write = Dialects.SNOWFLAKE)
        except Exception as e:
            raise Exception(f"Error converting view definition from {source_dialect} to Databricks dialect: {e}")
        return converted[0]
                 
    def get_external_volume(self, external_volume_name: str) -> SnowflakeExternalVolume:
        q = self._sql(f"DESCRIBE EXTERNAL VOLUME {external_volume_name}")
        for rec in q.fetchall():
            if rec['property'] == 'STORAGE_LOCATION_1':
                location_info = json.loads(rec['property_value'])
                return SnowflakeExternalVolume(
                        name=external_volume_name,
                        storage_provider=location_info["STORAGE_PROVIDER"],
                        storage_base_url=location_info["STORAGE_BASE_URL"]
                    )
            
    def get_external_volume_by_path(self, path: str) -> SnowflakeExternalVolume:
        """_summary_

        Args:
            path (str): _description_

        Raises:
            Exception: _description_

        Returns:
            SnowflakeExternalVolume: _description_
        """
        path = path
        logging.info(f"Inferring Snowflake external volume for path {path}")
        q = self.client.cursor(DictCursor).execute(f"SHOW EXTERNAL VOLUMES")
        for volume in q.fetchall():
            volume_info = self.get_external_volume(volume['name'])
            if volume_info.storage_base_url in path:
                return volume_info
        raise Exception(f"No external volume found for path {path}")

    def list_external_volumes(self) -> List[SnowflakeExternalVolume]:
        """List all Snowflake external volumes

        Returns:
            List[SnowflakeExternalVolume]: List of Snowflake External Volumes
        """
        volumes = []
        q = self._sql(f"SHOW EXTERNAL VOLUMES")
        for volume in q.fetchall():
            volumes.append(self.get_external_volume(volume['name']))
        
        return volumes
    
    def create_external_volume(self, name: str, 
                               storage_base_url: str, storage_provider: str = 'S3',
                               storage_aws_role_arn: str = None, 
                               storage_aws_external_id: str = None,
                               encryption: str = None, 
                               azure_tenant_id: str = None,
                               replace=False) -> SnowflakeExternalVolume:
        """Create a Snowflake external volume

        Args:
            name (str): _description_
            storage_base_url (str): _description_
            storage_provider (str, optional): _description_. Defaults to 'S3'.
            storage_aws_role_arn (str, optional): _description_. Defaults to None.
            storage_aws_external_id (str, optional): _description_. Defaults to None.
            encryption (str, optional): _description_. Defaults to None.
            azure_tenant_id (str, optional): _description_. Defaults to None.
            replace (bool, optional): _description_. Defaults to False.

        Returns:
            SnowflakeExternalVolume: Object representing the created external volume
        """
        create_stmt = (f"CREATE OR REPLACE EXTERNAL VOLUME {name}" 
                       if replace else f"CREATE EXTERNAL VOLUME IF NOT EXISTS {name}")
        sql =  self._sql(f"""{create_stmt}
                  STORAGE_LOCATIONS = (
                    ( 
                      NAME = '{name}'
                      STORAGE_PROVIDER = '{storage_provider}'
                      STORAGE_AWS_ROLE_ARN = '{storage_aws_role_arn}'
                      STORAGE_BASE_URL = '{storage_base_url}'
                    )
                  )""")
        return self.get_external_volume(name)


 