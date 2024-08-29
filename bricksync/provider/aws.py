from bricksync.provider import Provider
from bricksync.config import ProviderConfig
import boto3
import logging, os
from functools import cached_property


class AwsProvider(Provider):
    def __init__(self, provider_config: ProviderConfig):
        self.provider_config = provider_config

    @classmethod
    def initialize(cls, config: ProviderConfig):
        return cls(config)
    
    @cached_property
    def boto_session(self):
        return self.authenticate()
    
    def authenticate(self):
        if not self.provider_config.configuration:
            raise Exception("No configuration found for AWS provider. Minimally, region_name must be provided")
        
        if not self.provider_config.configuration.get('region_name'):
            raise Exception("No region_name found for AWS provider. Minimally, region_name must be provided")
        
        session = boto3.Session(
            aws_access_key_id=self.provider_config.configuration.get('aws_access_key_id'),
            aws_secret_access_key=self.provider_config.configuration.get('aws_secret_access_key'),
            profile_name=self.provider_config.configuration.get("profile_name"),
            region_name=self.provider_config.configuration.get("region_name")
        )

        if self.provider_config.configuration.get('profile_name'):
            os.environ["AWS_PROFILE"] = self.provider_config.configuration.get('profile_name')

        # Try go get ourself using sts to validate auth
        try:
           identity = session.client('sts').get_caller_identity()
        except:
            raise Exception(f"Failed to authenticate with AWS. Please check your credentials and configuration")
        logging.info(f"Authenticated with AWS with info: {identity}")
        return session