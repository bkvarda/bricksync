from bricksync import BrickSync
from bricksync.config import BrickSyncConfig, ProviderConfig
import tempfile, pytest


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
   with pytest.raises(Exception) as context:
        databricks_conf = bs.get_provider('databricks')
   assert "You may need to add to config" in str(context.value)
  

