from bricksync.provider.target import TargetProvider
from bricksync.config import ProviderConfig, TargetSyncStrategy, SyncTableType, TableFormat
from bricksync.table import Source, Target, TableSource, ViewSource, ViewTarget
import snowflake.connector as sf
from snowflake.connector import SnowflakeConnection
from snowflake.connector import DictCursor
from dataclasses import dataclass
from cloudpathlib import CloudPath
import ast, time, json, logging
from typing import Optional
import sqlglot
from sqlglot.dialects.dialect import Dialect, Dialects

@dataclass
class SnowflakeExternalVolume:
    name: str
    storage_provider: str
    storage_base_url: CloudPath

    def to_iceberg_metadata_string(self, full_iceberg_metadata_path: CloudPath):
        metadata_str = str(full_iceberg_metadata_path).split(str(self.storage_base_url))[1]
        if metadata_str[0] == '/':
            metadata_str = metadata_str[1:]
        return metadata_str
    
@dataclass
class SnowflakeExternalStage:
    name: str
    stage_location_url: CloudPath
    storage_integration: str
    iam_role: Optional[str] = None

    def to_stage_string(self, source_external_location: CloudPath) -> str:
        stage_name = f"@{self.name}/"
        if str(self.stage_location_url) not in str(source_external_location):
            raise Exception(f"Specified stage location {self.stage_location_url} is not in Source storage location {source_external_location}")
        return str(source_external_location).replace(str(self.stage_location_url), stage_name)

class SnowflakeTarget(TargetProvider):
    def __init__(self, provider_config: ProviderConfig = None, sync_strategy: TargetSyncStrategy = TargetSyncStrategy.MIRROR):
        self.provider_config = provider_config
        self.sync_strategy = sync_strategy
        self.client: SnowflakeConnection = self.authenticate()
        self.default_external_stage = (self.provider_config.configuration['default_external_stage'] 
                                            if 'default_external_stage' 
                                            in self.provider_config.configuration 
                                            else None)
        self.default_external_volume = (self.provider_config.configuration['default_external_volume']
                                        if 'default_external_volume'
                                        in self.provider_config.configuration
                                        else None)

    @classmethod
    def initialize(cls, provider_config: ProviderConfig, 
                   sync_strategy: TargetSyncStrategy = TargetSyncStrategy.MIRROR):
        return cls(provider_config, sync_strategy)
    
    
    def _get_full_target_name(self, source: Source) -> str:
        if self.sync_strategy.value == TargetSyncStrategy.MIRROR.value:
            return source.table_name
        
    def _infer_object_catalog(self) -> str:
        logging.info("Inferring Snowflake object catalog for Iceberg tables")
        q = self.client.cursor(DictCursor).execute(f"SHOW CATALOG INTEGRATIONS")
        for catalog in q.fetchall():
            q = self.client.cursor(DictCursor).execute(f"DESCRIBE CATALOG INTEGRATION {catalog['name']}")
            for rec in q.fetchall():
                if rec['property'] == 'CATALOG_SOURCE' and rec['property_value'] != 'OBJECT_STORE':
                    break
                elif rec['property'] == 'TABLE_FORMAT' and rec['property_value'] == 'ICEBERG':
                    logging.info(f"Found Iceberg catalog {catalog['name']}")
                    return catalog['name']
        logging.warning("No Snowflake Iceberg object catalog found. You will need to create one before syncing Iceberg tables")
        return None  
    
    def _infer_external_volume_info(self, source: TableSource) -> SnowflakeExternalVolume:
        logging.info(f"Infering Snowflake external volume for Iceberg table to be created from {source.table_name}")
        q = self.client.cursor(DictCursor).execute(f"SHOW EXTERNAL VOLUMES")
        for volume in q.fetchall():
            q = self.client.cursor(DictCursor).execute(f"DESCRIBE EXTERNAL VOLUME {volume['name']}")
            for rec in q.fetchall():
                if rec['property'] == 'STORAGE_LOCATION_1':
                    location_info = json.loads(rec['property_value'])
                    if location_info["STORAGE_BASE_URL"] in str(source.storage_location):
                        logging.info(f"Found Snowflake external volume {volume['name']} with matching base path {location_info['STORAGE_BASE_URL']}")
                        return SnowflakeExternalVolume(
                            name=volume['name'],
                            storage_provider=location_info["STORAGE_PROVIDER"],
                            storage_base_url=CloudPath(location_info["STORAGE_BASE_URL"])
                        )
        logging.warning(f"No Snowflake Iceberg external volume found with a base path found in {source.storage_location}")            
        return None 
    
    def _infer_external_stage_name(self, source: TableSource) -> str:
        # Get all stages available to the user and try to match the source storage location to one of them
         q = self.client.cursor(DictCursor).execute(f"SHOW STAGES")
         for rec in q.fetchall():
             if rec["url"] in str(source.storage_location) and len(rec["url"]) > 1:
                 return f"""{rec["database_name"]}.{rec["schema_name"]}.{rec["name"]}"""
         return None

    def _get_external_stage(self, source: TableSource) -> SnowflakeExternalStage:
        if self.default_external_stage:
            s = self.default_external_stage
        else:
            s = self._infer_external_stage_name(source)
        if not s:
            raise Exception(f"No external stage specified or inferred for Snowflake target of source{source}")
        # Validate storage integration actually exits
        stage_dict = {"name": s}
        try: 
            q = self.client.cursor(DictCursor).execute(f"DESCRIBE STAGE {s}")
            for rec in q.fetchall():
                if rec['property'] == 'URL':
                    stage_dict['stage_location_url'] = CloudPath(ast.literal_eval(rec['property_value'])[0])
                elif rec['property'] == 'STORAGE_INTEGRATION':
                    stage_dict['storage_integration'] = rec['property_value']
                elif rec['property'] == 'CREDENTIALS':
                    stage_dict['iam_role'] = rec['property_value'] 
        except Exception as e:
             raise Exception(f"Error while validating exterrnal stage {s} for sync targetof source {source}: {e}")
        return SnowflakeExternalStage(**stage_dict)
    
    def _get_view_query(self, source: ViewSource) -> str:
        converted_view_def = self.convert_view_dialect(source.view_definition, source.source_dialect)
        print(converted_view_def)
        return f"""CREATE OR REPLACE VIEW {source.table_name} AS {converted_view_def}"""
    
    def _get_ddl(self, source: Source) -> str:
        if source.type == SyncTableType.VIEW:
            return self._get_view_query(source)
        
        table_name = self._get_full_target_name(source)
        if source.type != SyncTableType.TABLE:
            raise Exception(f"Snowflake target does not support source type {source.type}")
        partition_keys_str = ""
        if source.partition_keys:
            quoted_keys = [f'"{key}"' for key in source.partition_keys]
            partition_keys_str = f"PARTITION BY ({','.join(quoted_keys)})"
        if source.table_format.value == TableFormat.DELTA.value:
            external_stage = self._get_external_stage(source)
            external_stage_str = external_stage.to_stage_string(source.storage_location)
            return f"""
            CREATE OR REPLACE EXTERNAL TABLE {table_name}
            {partition_keys_str} 
            LOCATION={external_stage_str}
            REFRESH_ON_CREATE=FALSE
            AUTO_REFRESH=FALSE
            FILE_FORMAT=(TYPE=PARQUET)
            TABLE_FORMAT=DELTA"""
        elif source.table_format.value == TableFormat.ICEBERG.value:
            catalog = self._infer_object_catalog()
            external_volume = self._infer_external_volume_info(source)
            metadata_file_path = external_volume.to_iceberg_metadata_string(source.iceberg_metadata_location)
            return f"""CREATE ICEBERG TABLE {table_name} 
            EXTERNAL_VOLUME='{external_volume.name}'
            CATALOG='{catalog}'
            METADATA_FILE_PATH='{metadata_file_path}'"""
        else:
            raise Exception(f"Unsupported table format {source.table_format}. Source: {source}")
        
    
    def _get_refresh_query(self, table_name: str, source: TableSource) -> str:
        if source.table_format.value == TableFormat.DELTA.value:
          return f"ALTER EXTERNAL TABLE {table_name} REFRESH"
        elif source.table_format.value == TableFormat.ICEBERG.value:
            external_volume = self._infer_external_volume_info(source)
            metadata_file_path = external_volume.to_iceberg_metadata_string(source.iceberg_metadata_location)
            return f"""ALTER ICEBERG TABLE {table_name} REFRESH '{metadata_file_path}'"""
        else:
            raise Exception(f"Unable to build refresh query: Unsupported table format {source.table_format}. Source: {source}")
    
    
    def _get_view_target(self, source: ViewSource, exists: bool) -> ViewTarget:
        view_name = self._get_full_target_name(source)
        logging.info(f"Getting Snowflake target info for view {source.table_name}")
        if exists:
             q = self.client.cursor(DictCursor).execute(f"SELECT GET_DDL('{source.type.value}','{view_name}', 'true') as ddl")
             return ViewTarget(
                    type = source.type,
                    table_name = view_name,
                    exists = True,
                    ddl = q.fetchall()[0]['DDL'],
                    refresh_query=None,
                    base_tables=[self._get_target(bt, True) for bt in source.base_tables]
                )
        else:
            ddl = self._get_ddl(source)
            logging.info(f"Generated ddl: {ddl}")
            return ViewTarget(
                type = source.type,
                table_name = view_name, 
                exists = False,
                ddl=ddl,
                refresh_query=None,
                base_tables=[self.get_target(bt) for bt in source.base_tables]
            )
        

    def _get_table_target(self, source: TableSource, exists: bool) -> Target:
         table_name = self._get_full_target_name(source)
         if exists:
             q = self.client.cursor(DictCursor).execute(f"SELECT GET_DDL('{source.type.value}','{table_name}', 'true') as ddl")
             return Target(
                 type = source.type,
                 table_name = table_name,
                 exists = True,
                 ddl = q.fetchall()[0]['DDL'],
                 refresh_query=self._get_refresh_query(table_name, source)
             )
         else:
             ddl = self._get_ddl(source)
             logging.info(f"Generated ddl: {ddl}")
             return Target(
                 type = source.type,
                 table_name = table_name,
                 exists = False,
                 ddl=ddl,
                 refresh_query=self._get_refresh_query(table_name, source)
             )

    def _get_target(self, source: Source, exists: bool) -> Target:
      if source.type == SyncTableType.VIEW:
        return self._get_view_target(source, exists)
      elif source.type == SyncTableType.TABLE:
        return self._get_table_target(source, exists)
      else:
        raise Exception(f"Unsupported source type {source.type}")
    
    def get_target(self, source: Source):
        # Check that source has all valid info needed for target determination
        table_name = self._get_full_target_name(source)
        logging.info(f"Getting Snowflake target info for source {source.table_name} of type {source.type.value}")
        # Check if target already exists
        try:
            q = self.client.cursor(DictCursor).execute(f"DESCRIBE TABLE {table_name}")
            return self._get_target(source, True)
        except Exception as e:
            if "does not exist" in str(e):
                # We need to create it
                pass
            else:
                raise
        return self._get_target(source, False)
    
    def _update_view_target(self, target: ViewTarget):
        if target.exists:
            logging.info(f"View target {target.table_name} already exists. Refreshing base tables")
            for bt in target.base_tables:
                self.update_target(bt)
        else:
            logging.info(f"Creating view target {target.table_name}")
            # First create the base tables
            for bt in target.base_tables:
                self.update_target(bt)
             # Works the same as a table 
            self._update_table_target(target)
    
    def _update_table_target(self, target: Target):
        logging.info(f"Updating Snowflake target {target.table_name}")
        target_db = target.table_name.split('.')[0]
        target_schema = target.table_name.split('.')[1]
        # If target exists, update it
        if target.exists:
            logging.info(f"Refreshing Snowflake target {target.table_name} that already exists")
            q = self.client.cursor()
            q.execute(target.refresh_query)
            return q.fetchall()

        # If target does not exist, create it
        else:
            logging.info(f"Snowflake {target.table_name} does not exist. Creating it.")
            q = self.client.cursor()
            # First make sure db and schema exists
            q.execute(f"CREATE DATABASE IF NOT EXISTS {target_db}")
            q.execute(f"CREATE SCHEMA IF NOT EXISTS {target_db}.{target_schema}")
            # Then create the object
            q.execute(target.ddl)
            return q.fetchall()
        return
    
    def update_target(self, target: Target):
        if target.type == SyncTableType.VIEW:
            return self._update_view_target(target)
        elif target.type == SyncTableType.TABLE:
            return self._update_table_target(target)
        else:
            raise Exception(f"Unsupported target type {target.type}")
        
    def convert_view_dialect(self, view_definition: str, source_dialect: Dialect) -> str:
        try:
          converted = sqlglot.transpile(view_definition, read = source_dialect, write = Dialects.SNOWFLAKE)
        except Exception as e:
            raise(f"Error converting view definition from {source_dialect} to Snowflake: {e}")
        return converted[0]

    
    def authenticate(self):
        try:
            client = (
            sf.connect(
                user=self.provider_config.configuration['user'] if 'user' in self.provider_config.configuration else None,
                password=self.provider_config.configuration['password'] if 'password' in self.provider_config.configuration else None,
                account=self.provider_config.configuration['account'] if 'account' in self.provider_config.configuration else None,
                warehouse=self.provider_config.configuration['warehouse'] if 'warehouse' in self.provider_config.configuration else None,
                database=self.provider_config.configuration['database'] if 'database' in self.provider_config.configuration else None,
                schema=self.provider_config.configuration['schema'] if 'schema' in self.provider_config.configuration else None,
                role=self.provider_config.configuration['role'] if 'role' in self.provider_config.configuration else None,
                session_parameters={
                    'QUOTED_IDENTIFIERS_IGNORE_CASE': 'True',
                }))
            return client
        except:
            raise