import pytest
from unittest.mock import MagicMock, patch
from snowflake.connector.cursor import DictCursor, SnowflakeCursor
from snowflake.connector import SnowflakeConnection
import json
from bricksync.provider.snowflake import SnowflakeProvider
from bricksync.provider.catalog.snowflake import (
SnowflakeCatalog, SnowflakeCatalogIntegration, 
SnowflakeExternalVolume,SnowflakeTableType)
from bricksync.config import ProviderConfig
from bricksync.table import IcebergTable, Table, View
from sqlglot.dialects import Dialects
import sqlglot
import sqlglot.expressions as exp

@pytest.fixture
def mock_config():
    config = MagicMock()
    config.configuration.get.side_effect = lambda key: {
        'user': 'test_user',
        'password': 'test_password',
        'account': 'test_account',
        'warehouse': 'test_warehouse',
        'database': 'test_database',
        'schema': 'test_schema',
        'role': 'test_role'
    }[key]
    return config

@patch('snowflake.connector.connect')
def test_authenticate_success(mock_connect, mock_config):
    # Mock the client
    mock_client = MagicMock()
    mock_connect.return_value = mock_client

    # Create an instance of the class
    instance = SnowflakeProvider(mock_config)

    # Call the authenticate method
    client = instance.authenticate()

    # Assertions
    mock_connect.assert_called_once_with(
        user='test_user',
        password='test_password',
        account='test_account',
        warehouse='test_warehouse',
        database='test_database',
        schema='test_schema',
        role='test_role',
        session_parameters={'QUOTED_IDENTIFIERS_IGNORE_CASE': 'True'}
    )

import pytest
from unittest.mock import MagicMock, patch
from bricksync.provider.snowflake import SnowflakeProvider
from bricksync.config import ProviderConfig
from snowflake.connector.cursor import DictCursor, SnowflakeCursor
from snowflake.connector import SnowflakeConnection
import json

def test_snowflake_table_type_enum():
    assert SnowflakeTableType.VIEW.value == "VIEW"
    assert SnowflakeTableType.TABLE.value == "TABLE"

def test_snowflake_catalog_integration_dataclass():
    integration = SnowflakeCatalogIntegration(
        name="test_integration",
        catalog_source="test_source",
        table_format="ICEBERG",
        enabled=True,
        refresh_interval_seconds=3600,
        comment="Test comment"
    )

    assert integration.name == "test_integration"
    assert integration.catalog_source == "test_source"
    assert integration.table_format == "ICEBERG"
    assert integration.enabled is True
    assert integration.refresh_interval_seconds == 3600
    assert integration.comment == "Test comment"

    # Test default values
    integration_default = SnowflakeCatalogIntegration(
        name="test_integration",
        catalog_source="test_source",
        table_format="ICEBERG",
        enabled=True
    )

    assert integration_default.refresh_interval_seconds is None
    assert integration_default.comment is None

def test_snowflake_external_volume_dataclass():
    volume = SnowflakeExternalVolume(
        name="test_volume",
        storage_provider="S3",
        storage_base_url="s3://bucket/path"
    )

    assert volume.name == "test_volume"
    assert volume.storage_provider == "S3"
    assert volume.storage_base_url == "s3://bucket/path"

def test_to_iceberg_metadata_string():
    volume = SnowflakeExternalVolume(
        name="test_volume",
        storage_provider="S3",
        storage_base_url="s3://bucket/path"
    )

    with pytest.raises(Exception):
        volume.to_iceberg_metadata_string("invalid_path")

    assert volume.to_iceberg_metadata_string("s3://bucket/path/metadata.json") == "metadata.json"

@patch('snowflake.connector.connect')
def test_snowflake_catalog_init(mock_connect):
    mock_provider = MagicMock(SnowflakeProvider)
    mock_provider.client = MagicMock(SnowflakeConnection)
    catalog = SnowflakeCatalog(provider=mock_provider)
    assert catalog.provider == mock_provider
    assert catalog.client == mock_provider.client

@patch('snowflake.connector.connect')
def test_snowflake_catalog_sql(mock_connect):
    mock_provider = MagicMock(SnowflakeProvider)
    mock_provider.client = MagicMock(SnowflakeConnection)
    catalog = SnowflakeCatalog(provider=mock_provider)
    mock_cursor = MagicMock(DictCursor)
    mock_provider.client.cursor.return_value = mock_cursor
    catalog._sql("SELECT 1")
    mock_cursor.execute.assert_called_once_with("SELECT 1")

@patch('snowflake.connector.connect')
def test_snowflake_catalog_format_describe_response(mock_connect):
    mock_provider = MagicMock(SnowflakeProvider)
    mock_provider.client = MagicMock(SnowflakeConnection)
    catalog = SnowflakeCatalog(provider=mock_provider)
    mock_cursor = MagicMock(DictCursor)
    mock_cursor.fetchall.return_value = [{'property': 'PROPERTY', 'property_value': 'value'}]
    response = catalog._format_describe_response(mock_cursor)
    assert response == {'property': 'value'}

@patch('snowflake.connector.connect')
def test_snowflake_catalog_get_iceberg_metadata_location(mock_connect):
    mock_provider = MagicMock(SnowflakeProvider)
    mock_provider.client = MagicMock(SnowflakeConnection)
    catalog = SnowflakeCatalog(provider=mock_provider)
    mock_cursor = MagicMock(DictCursor)
    mock_cursor.fetchone.return_value = {'ICEBERG_INFO': json.dumps({"metadataLocation": "s3://bucket/path/metadata"})}
    catalog._sql = MagicMock(return_value=mock_cursor)
    metadata_location = catalog._get_iceberg_metadata_location("test_table")
    assert metadata_location == "s3://bucket/path/metadata"


@patch('snowflake.connector.connect')
def test_get_table(mock_connect):
    mock_provider = MagicMock()
    mock_provider.client = MagicMock()
    catalog = SnowflakeCatalog(provider=mock_provider)
    
    # Mock the get_object_type method to return TABLE
    catalog.get_object_type = MagicMock(return_value=SnowflakeTableType.TABLE)
    
    # Mock the _sql method to return a cursor with the expected metadata
    mock_cursor = MagicMock()
    mock_cursor.fetchone.return_value = {'ICEBERG_INFO': '{"metadataLocation": "s3://bucket/path/metadata"}'}
    catalog._sql = MagicMock(return_value=mock_cursor)
    
    # Mock the _get_iceberg_metadata_location method to return a metadata location
    catalog._get_iceberg_metadata_location = MagicMock(return_value='s3://bucket/path/metadata')
    
    iceberg_table = catalog.get_table('test_table')
    
    assert isinstance(iceberg_table, IcebergTable)
    assert iceberg_table.name == 'test_table'
    assert iceberg_table.iceberg_metadata_location == 's3://bucket/path/metadata'

