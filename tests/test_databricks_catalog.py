import pytest
from unittest.mock import MagicMock, create_autospec
from pytest import fixture
from unittest.mock import patch
from bricksync.provider import  ProviderConfig
from bricksync.provider.databricks import DatabricksProvider
from bricksync.config import SyncConfig
from bricksync.provider.catalog.databricks import UniformIcebergInfo, DatabricksCatalog
from bricksync.table import IcebergTable
from databricks.sdk.service.catalog import (TableInfo, 
                                            TableType, 
                                            DataSourceFormat, ColumnInfo, 
                                            DependencyList, Dependency, 
                                            TableDependency)
from databricks.connect import DatabricksSession
from pyspark.sql.dataframe import DataFrame
from databricks.sdk.credentials_provider import credentials_strategy
from databricks.sdk import WorkspaceClient


@fixture
def table_sync_config():
    return SyncConfig(
        source="my.uc.table",
        source_provider="databricks",
        target_provider="snowflake",
    )

@fixture
def schema_sync_config():
    return SyncConfig(
        source="my.uc",
        source_provider="databricks",
        target_provider="snowflake",
    )

@fixture
def catalog_sync_config():
    return SyncConfig(
        source="my",
        source_provider="databricks",
        target_provider="snowflake",
    )


@fixture
def schema_table_info():
    return [TableInfo(full_name="my.uc.table1"),
            TableInfo(full_name="my.uc.table2")]

@fixture
def multi_schema_table_info():
    return [TableInfo(full_name="my.uc.table1"),
            TableInfo(full_name="my.uc.table2"),
            TableInfo(full_name="my.other.table1"),]

@fixture
def delta_table():
    return TableInfo(full_name="my.uc.delta_table",
                     table_type=TableType.MANAGED,
                     data_source_format=DataSourceFormat.DELTA,
                     storage_location="s3://my/delta_table",
                     columns=[ColumnInfo(name="col1",partition_index=None)])

@fixture 
def iceberg_table():
    return IcebergTable(
        name="my.uc.iceberg_table",
        storage_location="s3://my/iceberg_table",
        iceberg_metadata_location="s3://my/iceberg_table/metadata"
    )


@fixture
def delta_view():
    return TableInfo(full_name="my.uc.delta_view",
                     table_type=TableType.VIEW,
                     columns=[ColumnInfo(name="col1",partition_index=None)],
                     view_definition="select * from `my.uc.delta_table`",
                     view_dependencies=DependencyList(dependencies=[Dependency
                      (table =TableDependency(table_full_name="my.uc.delta_table") )])
                     )

@fixture
def delta_mv():
    return TableInfo(full_name="my.uc.delta_view",
                     table_type=TableType.MATERIALIZED_VIEW,
                     columns=[ColumnInfo(name="col1",partition_index=None)],
                     properties={"spark.internal.materialized_view.reconciliation_query": "select * from `my.uc.delta_table`"},
                     view_dependencies=DependencyList(dependencies=[Dependency
                      (table =TableDependency(table_full_name="my.uc.delta_table") )])
                     )
@fixture
def delta_view_nested():
    return TableInfo(full_name="my.uc.delta_with_nested_view",
                     table_type=TableType.VIEW,
                     columns=[ColumnInfo(name="col1",partition_index=None)],
                     view_definition="select * from `my.uc.delta_table`",
                     view_dependencies=DependencyList(
                         dependencies=
                         [
                           Dependency(table = TableDependency(table_full_name="my.uc.delta_table")),
                           Dependency(table = TableDependency(table_full_name="my.uc.delta_view"))
                         ]
                     ))

@patch("databricks.connect.DatabricksSession")
def get_spark_client(mocker):
    spark = create_autospec(DatabricksSession)
    spark.sql = MagicMock(return_value="")
    return spark

@credentials_strategy('noop', [])
def noop_credentials(_: any):
    return lambda: {}

@pytest.fixture
def databricks_catalog(mocker):
    w = create_autospec(WorkspaceClient)
    spark = get_spark_client()
    mocker.patch("bricksync.provider.databricks.DatabricksProvider.authenticate", return_value=w)
    mocker.patch("bricksync.provider.databricks.DatabricksProvider._get_spark_client", return_value=spark)
    dsp = DatabricksProvider(ProviderConfig("databricks", configuration={"cluster_id": "12345"}))
    return DatabricksCatalog(dsp)

def test_get_table(databricks_catalog, delta_table):
    databricks_catalog.client.tables.get.return_value = delta_table
    table = databricks_catalog.get_table("my.uc.delta_table")
    assert table.name == "my.uc.delta_table"
    assert table.storage_location == "s3://my/delta_table"
    assert table.is_delta()
    assert not table.is_iceberg()

def test_get_uniform_table(databricks_catalog, delta_table):
    databricks_catalog.client.tables.get.return_value = delta_table
    databricks_catalog.client.api_client.do.return_value = {
        "delta_uniform_iceberg": {
            "metadata_location": "s3://my/metadata",
            "converted_delta_version": "0.8.0",
            "converted_delta_timestamp": "2021-06-01T00:00:00Z"
        }

    }
    table = databricks_catalog.get_table("my.uc.delta_table")
    assert table.name == "my.uc.delta_table"
    assert table.storage_location == "s3://my/delta_table"
    assert table.is_delta()
    assert table.is_iceberg()
    assert table.uniform_iceberg_info.metadata_location == "s3://my/metadata"
    assert table.to_iceberg_table().name == "my.uc.delta_table"
    assert table.to_iceberg_table().storage_location == "s3://my/delta_table"

def test_non_uniform_table_to_iceberg(databricks_catalog, delta_table):
    databricks_catalog.client.tables.get.return_value = delta_table
    databricks_catalog.client.api_client.do.return_value = {}
    table = databricks_catalog.get_table("my.uc.delta_table")
    assert table.name == "my.uc.delta_table"
    assert table.storage_location == "s3://my/delta_table"
    assert table.is_delta()
    assert not table.is_iceberg()
    with pytest.raises(Exception) as context:
        table.to_iceberg_table()
    assert "Table my.uc.delta_table does not have Iceberg metadatata" in str(context.value)

def test_get_view(databricks_catalog, delta_view, delta_table):
    databricks_catalog.client.tables.get.side_effect = [delta_view, delta_table]
    view = databricks_catalog.get_table("my.uc.delta_view")
    assert view.name == "my.uc.delta_view"
    assert view.is_view()
    assert "my.uc.delta_table" == view.base_tables[0].name
    assert view.base_tables[0].is_delta()

def test_get_mv(databricks_catalog, delta_mv, delta_table):
    databricks_catalog.client.tables.get.side_effect = [delta_mv, delta_table]
    mv = databricks_catalog.get_table("my.uc.delta_view")
    assert mv.name == "my.uc.delta_view"
    assert mv.is_view()
    assert "my.uc.delta_table" == mv.base_tables[0].name
    assert mv.base_tables[0].is_delta()

def test_get_nested_view(databricks_catalog, delta_view_nested, delta_view, delta_table):
    databricks_catalog.client.tables.get.side_effect = [delta_view_nested, delta_view, delta_table]
    view = databricks_catalog.get_table("my.uc.delta_with_nested_view")
    assert view.name == "my.uc.delta_with_nested_view"
    assert view.is_view()
    names = [t.name for t in view.base_tables]
    assert "my.uc.delta_table" in names

def test_create_catalog_schema(databricks_catalog):
    databricks_catalog.create_catalog("my_catalog")
    databricks_catalog.client.catalogs.create.assert_called_with("my_catalog")
    databricks_catalog.create_schema("my_catalog", "my_schema")
    databricks_catalog.client.schemas.create.assert_called_with("my_schema", catalog_name="my_catalog")

def test_reverse_uniform(databricks_catalog, iceberg_table):
    databricks_catalog.sql = MagicMock(return_value="")
    databricks_catalog.create_or_refresh_external_table(iceberg_table)
    statement = f"""CREATE TABLE IF NOT EXISTS my.uc.iceberg_table
            UNIFORM iceberg
            METADATA_PATH 's3://my/iceberg_table/metadata';
            REFRESH TABLE my.uc.iceberg_table METADATA_PATH 's3://my/iceberg_table/metadata';
            """ 
    databricks_catalog.sql.assert_called_with(statement)




    



