import pytest
from unittest import mock
from unittest.mock import MagicMock
from pytest import fixture
from unittest.mock import patch, MagicMock
from bricksync.provider import  ProviderConfig
from bricksync.provider.databricks import DatabricksProvider
from bricksync.config import SyncConfig
from bricksync.provider.catalog.databricks import UniformIcebergInfo, DatabricksCatalog
from databricks.sdk.service.catalog import (TableInfo, 
                                            SchemaInfo, 
                                            TableType, 
                                            DataSourceFormat, ColumnInfo, 
                                            DependencyList, Dependency, 
                                            TableDependency)
from databricks.sdk import WorkspaceClient
from databricks.connect import DatabricksSession


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
                     view_dependencies=DependencyList(dependencies=[Dependency
                      (table =TableDependency(table_full_name="my.uc.delta_table") ),
                      Dependency(table_full_name="my.uc.delta_view")])
                     )

@patch("databricks.connect.DatabricksSession")
def get_spark_client(mocker):
    spark = DatabricksSession.builder.clusterId("12345").getOrCreate()
    spark._jsc = MagicMock()
    spark._sc = MagicMock()
    spark._jvm = MagicMock()
    return spark



def dsp(mocker):
    w = WorkspaceClient(host="https://my.databricks.com", token="mytoken")
    spark = get_spark_client()
    mocker.patch("bricksync.provider.databricks.DatabricksProvider.authenticate", return_value=w)
    mocker.patch("bricksync.provider.databricks.DatabricksProvider._get_spark_client", return_value=spark)
    dsp = DatabricksProvider(ProviderConfig("databricks", configuration={"cluster_id": "12345"}))

    for attr in vars(dsp.client):
        if attr == "_config":
            continue
        else:
            setattr(dsp.client, attr, MagicMock())
    return dsp

@fixture
def databricks_catalog(mocker):
    databricks_provider = dsp(mocker)
    return DatabricksCatalog(databricks_provider)

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



    



