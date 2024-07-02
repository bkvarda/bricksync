from abc import ABC, ABCMeta, abstractmethod
from typing import Union, Tuple
from bricksync.provider import Provider
from bricksync.table import Table, View
from bricksync.exceptions import UnsupportedTableTypeError
from sqlglot.dialects.dialect import Dialect
import sqlglot
import sqlglot.expressions as exp

class CatalogProvider():
    @abstractmethod
    def get_table(self) -> Union[Table, View]:
        pass
    
    @abstractmethod
    def create_catalog(self, catalog_name: str):
        pass
    
    @abstractmethod
    def create_schema(self, catalog_name: str, schema_name: str):
        pass
    
    @abstractmethod
    def create_or_refresh_external_table(self, table: Table):
        pass
    
    @abstractmethod
    def create_or_refresh_view(self, view: View):
        pass

    @abstractmethod
    def convert_view_dialect(self, view_definition: str, source_dialect: Dialect):
        pass

    def get_fqtn_parts(self, table: Union[Table, View, str]) -> Tuple[str]:
        if type(table) is str:
            return tuple(table.split("."))
        else:
            return tuple(table.name.split("."))
    
    def get_catalog_from_name(self, table: Union[Table, View, str]) -> str:
        parts = self.get_fqtn_parts(table)
        if len(parts) == 3:
            return parts[0]
        else:
            return None
          
    def get_schema_from_name(self, table: Union[Table, View, str]) -> str:
        parts = self.get_fqtn_parts(table)
        if len(parts) == 3:
            return parts[1]
        else:
            return parts[0]
        
    def get_table_from_name(self, table: Union[Table, View, str]) -> str:
        parts = self.get_fqtn_parts(table)
        if len(parts) == 3:
            return parts[2]
        else:
            return parts[1]
        
    def replace_table_identifiers(self, table: Table, 
                              catalog_name: str = None, 
                              schema_name: str = None, table_name: str = None) -> Table:
    
      if not (catalog_name or schema_name or table_name):
          return table
      
      current_catalog = self.get_catalog_from_name(table)
      current_schema = self.get_schema_from_name(table)
      current_table_name = self.get_table_from_name(table)

      if current_catalog == catalog_name and current_schema == schema_name and current_table_name == table_name:
          return table

      new_catalog_name = catalog_name if catalog_name else current_catalog
      new_schema_name = schema_name if schema_name else current_schema
      new_table_name = table_name if table_name else current_table_name

      table.set_name(f"{new_catalog_name}.{new_schema_name}.{new_table_name}")
      return table
      

    def replace_view_identifiers(self, view: View, 
                                  catalog_name: str = None, 
                                  schema_name: str = None, table_name: str = None,
                                  base_table_catalog_name: str = None,
                                  base_table_schema_name: str = None) -> View:
        view_name = view.name
        expression = sqlglot.parse_one(view.view_definition, read=view.dialect)
        base_table_list = list(expression.find_all(exp.Table))
        base_table_list_fmt = [f"{bt.catalog}.{bt.db}.{bt.name}" for bt in base_table_list]
        base_tables = [self.replace_table_identifiers(bt, base_table_catalog_name, base_table_schema_name, None) 
                            for bt in base_table_list_fmt if bt not in [view_name.upper(), view_name.lower()]]
        return
    
    def replace_identifiers(self, table: Union[Table, View], 
                            catalog_name: str = None, 
                            schema_name: str = None, table_name: str = None,
                            base_table_catalog_name: str = None, 
                            base_table_schema_name: str = None) -> Union[Table, View]:
        
        if not (catalog_name and schema_name and table_name):
            return table
        
        if isinstance(table, Table):
            return self.replace_table_identifiers(table, catalog_name, schema_name, table_name)
        elif isinstance(table, View):    
            return self.replace_view_identifiers(table, catalog_name, schema_name, table_name)
        else:
            raise UnsupportedTableTypeError(f"Unsupported table type: {type(table)}")
        
        return
    



