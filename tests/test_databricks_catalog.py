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
from databricks import sql
from databricks.sql.thrift_api.TCLIService.ttypes import TOpenSessionResp


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

DUMMY_CONNECTION_ARGS = {
        "server_hostname": "foo",
        "http_path": "dummy_path",
        "access_token": "tok",
    }


@patch("databricks.sql.client.ThriftBackend")
def get_sql_client(mocker):
    instance = mocker.return_value
    mock_open_session_resp = MagicMock(spec=TOpenSessionResp)()
    mock_open_session_resp.sessionHandle.sessionId = b'\x22'
    instance.open_session.return_value = mock_open_session_resp
    connection = sql.connect(**DUMMY_CONNECTION_ARGS)
    return connection



def dsp(mocker):
    w = WorkspaceClient(host="https://my.databricks.com", token="mytoken")
    sql_client = get_sql_client()
    mocker.patch("bricksync.provider.databricks.DatabricksProvider.authenticate", return_value=w)
    mocker.patch("bricksync.provider.databricks.DatabricksProvider._get_sql_client", return_value=sql_client)
    dsp = DatabricksProvider(ProviderConfig("databricks", ProviderConfigType.SOURCE, configuration={"cluster_id": "12345"}))

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
    assert table.is_delta() == True


    



