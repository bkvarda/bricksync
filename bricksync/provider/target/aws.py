from bricksync.provider import Provider
from bricksync.provider.target import TargetProvider
from bricksync.config import ProviderConfig, TargetSyncStrategy
from bricksync.table import Source, Target
from sqlglot.dialects.dialect import Dialect
import boto3
import logging

class AwsTarget(TargetProvider):
    def __init__(self, provider_config: ProviderConfig, target_sync_strategy: TargetSyncStrategy):
        self.provider_config = provider_config
        self.boto_session = self.authenticate()

    @classmethod
    def initialize(cls, config: ProviderConfig, target_sync_strategy: TargetSyncStrategy):
        return cls(config, target_sync_strategy)
    
    def authenticate(self):
        if not self.provider_config.configuration:
            raise("No configuration found for AWS provider. Minimally, region_name must be provided")
        
        if not self.provider_config.configuration['region_name']:
            raise("No region_name found for AWS provider. Minimally, region_name must be provided")
        
        session = boto3.Session(
            aws_access_key_id=self.provider_config.configuration['aws_access_key_id'] if 'aws_access_key_id' in self.provider_config.configuration else None,
            aws_secret_access_key=self.provider_config.configuration['aws_secret_access_key'] if 'aws_secret_access_key' in self.provider_config.configuration else None,
            profile_name=self.provider_config.configuration['profile_name'] if 'profile_name' in self.provider_config.configuration else None,
            region_name=self.provider_config.configuration['region_name']
        )
        # Try go get ourself using sts to validate auth
        try:
           identity = session.client('sts').get_caller_identity()
        except:
            raise(f"Failed to authenticate with AWS. Please check your credentials and configuration")
        logging.info(f"Authenticated with AWS with info: {identity}")
        return session

    def get_target(self):
        pass
    def update_target(self):
        pass
    def convert_view_dialect(self, view_definition: str, source_dialect: Dialect):
        pass

class AthenaTarget(AwsTarget):
    def __init__(self, provider_config: ProviderConfig, target_sync_strategy: TargetSyncStrategy):
        super().__init__(provider_config, target_sync_strategy)
        self.client = self._get_athena_client()
    def _get_athena_client(self):
        return self.boto_session.client('athena')    
    def get_target(self, source: Source):
        #TODO
        return
    def update_target(self, target: Target):
        #TODO
        return
    def convert_view_dialect(self, view_definition: str, source_dialect: Dialect):
        #TODO
        return
    
class GlueTarget(AwsTarget):
    def __init__(self, provider_config: ProviderConfig, target_sync_strategy: TargetSyncStrategy):
        super().__init__(provider_config, target_sync_strategy)
        self.client = self._get_glue_client()
    def _get_glue_client(self):
        return self.boto_session.client('glue')
    def get_target(self, source: Source):
        #TODO
        return
    def update_target(self, target: Target):
        #TODO
        return
    def convert_view_dialect(self, view_definition: str, source_dialect: Dialect):
        #TODO
        return