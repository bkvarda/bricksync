from bricksync.provider import Provider
from bricksync.config import ProviderConfig
from snowflake.connector import SnowflakeConnection
import snowflake.connector as sf

class SnowflakeProvider(Provider):
    def __init__(self, provider_config: ProviderConfig):
        self.provider_config = provider_config
        self.client: SnowflakeConnection = self.authenticate()
    
    @classmethod
    def initialize(cls, provider_config: ProviderConfig):
        return cls(provider_config)
    
    def authenticate(self):
        try:
            client = (
            sf.connect(
                user=self.provider_config.configuration.get('user'),
                password=self.provider_config.configuration.get('password'),
                account=self.provider_config.configuration.get('account'),
                warehouse=self.provider_config.configuration.get('warehouse'),
                database=self.provider_config.configuration.get('database'),
                schema=self.provider_config.configuration.get('schema'),
                role=self.provider_config.configuration.get('role'),
                session_parameters={
                    'QUOTED_IDENTIFIERS_IGNORE_CASE': 'True',
                }))
            return client
        except:
            raise
