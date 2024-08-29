from bricksync.provider import Provider
from bricksync.config import ProviderConfig
from databricks.sdk import WorkspaceClient
from databricks.connect import DatabricksSession as SparkSession
import os, logging
from functools import cached_property

class DatabricksProvider(Provider):
    def __init__(self, provider_config: ProviderConfig):
        self.provider_config = provider_config
        self.client: WorkspaceClient = self.authenticate()
        self.spark: SparkSession = self._get_spark_client()
    @classmethod
    def initialize(cls, provider_config: ProviderConfig):
        return cls(provider_config)
    
    @cached_property
    def authenticate(self):
        try:
          client = (WorkspaceClient() if not self.provider_config.configuration else
              WorkspaceClient(
              host=self.provider_config.configuration.get('host'),
              token=self.provider_config.configuration.get('token'),
              profile=self.provider_config.configuration.get('profile'))
          )
          logging.info(f"Databricks provider API authenticated user info: {client.current_user.me().as_dict()}")
          return client
        except: 
          raise
       
    @cached_property
    def _get_spark_client(self):
        # If we're current running on serverless, use current serverless
        if os.environ.get("IS_SERVERLESS") == "TRUE":
            logging.info("Databricks provider detected DBR serverless environment. Using current spark session.")
            return SparkSession.builder.getOrCreate()
        # If we're on another kind of cluster, use current cluster
        elif os.environ.get("DATABRICKS_RUNTIME_VERSION"):
            logging.info("Databricks provider detected DBR cluster environment. Using current spark session.")
            return SparkSession.builder.getOrCreate()
        # If we want to connect to a specific cluster
        elif self.provider_config.configuration.get("cluster_id"):
            logging.info("Databricks provider connecting to supplied cluster_id for spark session")
            return (SparkSession.builder
                    .clusterId(self.provider_config.configuration.get("cluster_id"))
                    .profile(self.provider_config.configuration.get('profile'))
                    .token(self.provider_config.configuration.get('token'))
                    .host(self.provider_config.configuration.get('host'))
                    .getOrCreate())
        # Otherwise use serverless by default
        else:
            logging.info("Databricks provider connecting remotely to serverless environment for spark session.")
            return (SparkSession.builder
                    .serverless(True)
                    .profile(self.provider_config.configuration.get('profile'))
                    .token(self.provider_config.configuration.get('token'))
                    .host(self.provider_config.configuration.get('host'))
                    .getOrCreate())

        

           
           