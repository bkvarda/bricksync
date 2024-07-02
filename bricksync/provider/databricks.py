from bricksync.provider import Provider
from bricksync.config import ProviderConfig
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config, oauth_service_principal
from databricks.connect.session import DatabricksSession, SparkSession
from databricks import sql
from databricks.sql.client import Connection
from cachetools import cachedmethod
import os

class DatabricksProvider(Provider):
    def __init__(self, provider_config: ProviderConfig):
        self.provider_config = provider_config
        self.client: WorkspaceClient = self.authenticate()
        self.cluster_id = self.provider_config.configuration.get("cluster_id")
        self.spark: SparkSession = ( DatabricksSession.builder
                                    .profile(self.client.config.profile)
                                    .clusterId(next((item for item in [self.client.config.cluster_id, self.cluster_id] if item is not None), None))
                                    .getOrCreate())
        self.sql_client: Connection = self._get_sql_client()
    @classmethod
    def initialize(cls, provider_config: ProviderConfig):
        return cls(provider_config)
    
    def authenticate(self):
        try:
          client = (WorkspaceClient() if not self.provider_config.configuration else
              WorkspaceClient(
              host=self.provider_config.configuration.get('host'),
              token=self.provider_config.configuration.get('token'),
              profile=self.provider_config.configuration.get('profile'))
          )
          print(client.current_user.me())
          return client
        except: 
          raise

    def _get_access_token(self):
       if self.provider_config.configuration.get("token"):
            return self.provider_config.configuration.get("token")
       
       token = self.client.config.authenticate().get("Authorization")
       if 'Bearer ' in token:
          return token.split(" ")[1]
       else:
          return None
       
    def _get_oauth_config(self):
        client_id = self.provider_config.configuration.get("client_id")
        client_secret = self.provider_config.configuration.get("client_secret")
        client_id_env = os.environ.get("DATABRICKS_CLIENT_ID")
        client_secret_env = os.environ.get("DATABRICKS_CLIENT_SECRET")
        if not (client_id and client_secret) and not (client_id_env and client_secret_env):
          return None
        else:
           conf = Config(
              host = self.client.config.host,
              client_id = client_id if client_id else client_id_env,
              client_secret = client_secret if client_secret else client_secret_env,              
           )
           return oauth_service_principal(conf) 
               
    def _create_or_retrieve_optimal_warehouse(self):
       warehouses = self.client.warehouses.list()
       serverless_wh = [wh for wh in warehouses if wh.enable_serverless_compute]
       if len(serverless_wh) > 0:
          # TODO - Find a good one
          # For now just return the first
          for warehouse in warehouses:
              return warehouse          
       else:
          # Create a little serverless one
          warehouse = self.client.warehouses.create_and_wait(
                name="brick_sync",
                min_num_clusters=1,
                max_num_clusters=1,
                auto_stop_mins=1,
                enable_serverless_compute=True,
                cluster_size="Small",
          )
          return warehouse
       
    def _get_sql_client(self):
        if self.provider_config.configuration.get("warehouse_id"):
           warehouse = self.client.warehouses.get(self.provider_config.configuration.get("warehouse_id"))
           return sql.connect(
              server_hostname=warehouse.odbc_params.hostname,
              server_port=warehouse.odbc_params.port,
              http_path=warehouse.odbc_params.path,
              access_token=self._get_access_token(),
              credentials_provider=self._get_oauth_config())
           
        else:
           warehouse = self._create_or_retrieve_optimal_warehouse()
           return sql.connect(
              server_hostname=warehouse.odbc_params.hostname,
              server_port=warehouse.odbc_params.port,
              http_path=warehouse.odbc_params.path,
              access_token=self._get_access_token(),
              credentials_provider=self._get_oauth_config())
           
           