from bricksync.provider.catalog import CatalogProvider
from bricksync.provider.aws import AwsProvider
from bricksync.table import Table, View, IcebergTable
from typing import Union
from sqlglot.dialects.dialect import Dialect
from pyiceberg.catalog import glue
from pyiceberg.serializers import FromInputFile
from pyiceberg import table
from pyiceberg import exceptions
from bricksync.provider import ProviderConfig
import logging

logging.getLogger(__name__)

class GlueCatalog(CatalogProvider):
    def __init__(self, provider: AwsProvider):
        self.provider = provider
        self.session = provider.boto_session
        self.client = glue.GlueCatalog("glue", **provider.provider_config.configuration)
        self.target_catalog_name = self.provider.provider_config.configuration.get("catalog_name", "glue_catalog")


    @classmethod
    def initialize(cls, provider_config: ProviderConfig):
        return cls(provider=AwsProvider.initialize(provider_config))

    def _pyiceberg_table_to_table(self, table: table.Table):
        return IcebergTable(
            name=".".join(elem for elem in table.name()),
            storage_location=table.location(),
            iceberg_metadata_location=table.metadata_location
        )
    
    def _list_tables(self, database: str):
        return self.client.list_tables(database)
    
    def get_table(self, name: str) -> Union[Table, View]:
        table_parts = self.get_fqtn_parts(name)
        if len(table_parts) == 3:
            schema, table_name = table_parts[1:]
        else:
            schema = table_parts[0]
            table_name = table_parts[1]
        glue_table_name = f"{schema}.{table_name}"
        tbl = self.client.load_table(glue_table_name)
        return self._pyiceberg_table_to_table(tbl)
    
    def create_catalog(self, catalog_name: str):
        # Glue does not have a concept of custom catalog, catalog is basically acct id
        # For now, just ignoring any specified catalog name
        return
    
    def create_schema(self, catalog_name: str, schema_name: str):
        try:
            self.client.create_namespace(schema_name)
        except Exception as e:
            if 'already exists' in str(e):
              logging.info(f"Schema {schema_name} already exists, skipping creation.")
              pass
            else:
              raise(e)
        return
    
    def refresh_external_table(self, schema: str, table_name: str, metadata_location: str):
        glue_table = self.client._get_glue_table(schema, table_name)
        glue_table_name = f"{schema}.{table_name}"
        glue_table_version_id = glue_table.get("VersionId")
        prev_metadata_location = glue_table.get("Parameters").get("metadata_location")
        logging.info(f"Glue table {table_name} previous metadata location: {prev_metadata_location}")
        logging.info(f"Glue table {table_name} new metadata location: {metadata_location}")
        if prev_metadata_location == metadata_location:
           logging.info(f"Metadata location for {table_name} has not changed, skipping refresh.")
           return self.get_table(glue_table_name)
        
        io = self.client._load_file_io(location=metadata_location)
        file = io.new_input(metadata_location)
        metadata = FromInputFile.table_metadata(file)
        
        update_table_req = glue._construct_table_input(
            table_name=table_name,
            metadata_location=metadata_location,
            glue_table=glue_table,
            metadata=metadata,
            properties={}
        )

        self.client._update_glue_table(
            database_name=schema,
            table_name=table_name,
            table_input=update_table_req,
            version_id=glue_table_version_id
        )
        return self.get_table(glue_table_name)

         
    def create_or_refresh_external_table(self, table: Union[Table, View]):
        if table.is_view():
            raise NotImplementedError(f"GlueCatalog does not support creating or refreshing views currently")
        if not table.is_iceberg():
            raise NotImplementedError(f"GlueCatalog does not yet support non-Iceberg tables: {table.name} is not an Iceberg table")
        schema = self.get_schema_from_name(table)
        table_name = self.get_table_from_name(table)
        glue_table_name = f"{schema}.{table_name}"
        try:
            self.get_table(table.name)
        except exceptions.NoSuchTableError:
            # Table does not exist, need to create it
            self.client.register_table(glue_table_name, table.iceberg_metadata_location)
            return self.get_table(table.name)
        except FileNotFoundError:
            # Table was found but metadata location might no longer exist for some reason
            # Assumption that table needs to be refreshed
            pass
        except Exception as e:
            raise(e)
        # Table exists, need to refresh it
        return self.refresh_external_table(schema, table_name, table.iceberg_metadata_location)

    def create_or_refresh_view(self, view: View):
        raise NotImplementedError("GlueCatalog does not support creating or refreshing views currently")
        pass

    def convert_view_dialect(self, view_definition: str, source_dialect: Dialect):
        raise NotImplementedError("GlueCatalog does not support creating or refreshing views currently")
        pass
    