from dataclasses import dataclass
from typing import List, Dict, Union, Optional, Tuple
from sqlglot.dialects.dialect import Dialect

@dataclass
class Table():
    name: str
    storage_location: str

    def is_view(self) -> bool:
        return False
    
    def is_table(self) -> bool:
        return True
    
    def is_iceberg(self) -> bool:
        return False
    
    def is_delta(self) -> bool:
        return False
    
    def set_name(self, name: str):
        self.name = name
    

@dataclass
class UniformIcebergInfo:
    metadata_location: str
    converted_delta_version: int
    converted_delta_timestamp: str

@dataclass
class IcebergTable(Table):
    iceberg_metadata_location: str

    def is_iceberg(self) -> bool:
        return True

@dataclass
class DeltaTable(Table):
    delta_properties: Dict[str, str]
    uniform_iceberg_info: Optional[UniformIcebergInfo]

    def is_iceberg(self) -> bool:
        return self.uniform_iceberg_info is not None
    
    def is_delta(self) -> bool:
        return True
    
    def to_iceberg_table(self) -> IcebergTable:
        if not self.is_iceberg():
            raise Exception(f"Table {self.name} does not have Iceberg metadatata")

        return IcebergTable(name=self.name, 
                            storage_location=self.storage_location, 
                            iceberg_metadata_location=self.uniform_iceberg_info.metadata_location)

@dataclass
class ViewSource():
    name: str

    def is_view(self) -> bool:
        return True
    
    def is_table(self) -> bool:
        return False
    
    def set_name(self, name: str):
        self.name = name

@dataclass
class View(ViewSource):
    view_definition: str
    dialect: Dialect
    base_tables: List[Union[Table, ViewSource]]

    def set_view_definition(self, view_definition: str):
        self.view_definition = view_definition

    def set_base_tables(self, base_tables: List[Union[Table, ViewSource]]):
        self.base_tables = base_tables

