from bricksync import BrickSync
from bricksync.config import BrickSyncConfig, ProviderType, ProviderConfig
import tempfile, pytest
from unittest.mock import MagicMock, create_autospec, patch
from bricksync.provider.databricks import DatabricksProvider
from bricksync.provider.catalog.databricks import DatabricksCatalog
from bricksync.provider.catalog.snowflake import SnowflakeCatalog
from bricksync.provider.catalog import Table, CatalogProvider
from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient
from databricks.sdk.credentials_provider import credentials_strategy


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

@pytest.fixture
def mock_config():
    config = MagicMock(spec=BrickSyncConfig)
    config.providers = [{'databricks': ProviderConfig(ProviderType.DATABRICKS, configuration={})}]
    return config

@pytest.fixture
def bricksync(mock_config, databricks_catalog):
    with patch("bricksync.BrickSync._initialize_provider") as mock_init:
        mock_init.return_value = databricks_catalog
        return BrickSync(mock_config)

def test_load_conf():
    with tempfile.NamedTemporaryFile(delete=False) as fp:
      fp.write(b"""providers:
  - databricks:
       provider: databricks
       type: source
       configuration:
         profile: test-profile
         cluster_id: "12346"
  - snowflake:
        provider: snowflake
        configuration:
          account: some-account
          user: someuser
          password: somepassword
  - glue:
        provider: glue
        configuration:
          region_name: us-west-2
          profile_name: some-profile-name
          s3.region: us-west-2""")    
      fp.close()
      bs = BrickSyncConfig.load(fp.name)
      databricks_config = bs.get_provider_config("databricks")
      snowflake_config = bs.get_provider_config("snowflake")
      glue_config = bs.get_provider_config("glue")
      assert databricks_config.configuration['profile'] == 'test-profile'
      assert snowflake_config.configuration['account'] == 'some-account'
      assert glue_config.configuration['region_name'] == 'us-west-2'
      assert glue_config.configuration['profile_name'] == 'some-profile-name'
      assert glue_config.configuration['s3.region'] == 'us-west-2'

def test_add_provider_conf():
    bs = BrickSyncConfig.new()
    bs.add_provider('databricks', ProviderConfig("databricks", {'profile': 'test-profile', 'cluster_id': '12346'}))
    databricks_conf = bs.get_provider_config('databricks')
    assert databricks_conf.configuration['profile'] == 'test-profile'
    assert databricks_conf.configuration['cluster_id'] == '12346'

def test_add_bs_provider_lazy():
   bs = BrickSync.new()
   bs.add_provider('databricks', "databricks", {'profile': 'test-profile', 'cluster_id': '12346'}, lazy_init=True)
   assert bs.initialized['databricks'] == False
   with patch("bricksync.provider.databricks.DatabricksProvider.authenticate") as mock_auth:
       with patch("bricksync.provider.databricks.DatabricksProvider._get_spark_client") as mock_spark:
           bs.get_provider('databricks')
           assert bs.initialized['databricks'] == True

def test_add_bs_provider_lazy_init():
    bs = BrickSync.new()
    bs.add_provider('databricks', "databricks", {'profile': 'test-profile', 'cluster_id': '12346'}, lazy_init=True)
    assert bs.initialized['databricks'] == False
    with patch("bricksync.provider.databricks.DatabricksProvider.authenticate") as mock_auth:
        with patch("bricksync.provider.databricks.DatabricksProvider._get_spark_client") as mock_spark:
            bs.initialize()
            assert bs.initialized['databricks'] == True

def test_sync(bricksync):
    source_provider_name = 'databricks'
    target_provider_name = 'snowflake'
    source_table_name = 'source_table'
    target_table_name = 'target_table'
    
    source_provider = MagicMock(spec=DatabricksProvider)
    target_provider = MagicMock(spec=DatabricksProvider)
    source_table = MagicMock(spec=Table)
    
    bricksync.get_provider = MagicMock(side_effect=lambda name: source_provider if name == source_provider_name else target_provider)
    source_provider.get_table = MagicMock(return_value=source_table)
    
    with patch.object(bricksync, '_sync') as mock_sync:
        bricksync.sync(source_provider_name, source_table_name, target_provider_name, target_table_name)
        assert mock_sync.called
        assert mock_sync.call_count == 1

       



