
from bricksync.config import BrickSyncConfig, ProviderType, ProviderConfigType, ProviderConfig, SyncConfig
from bricksync.provider import Provider
from bricksync.provider.source.databricks import DatabricksSource
from bricksync.provider.target.snowflake import SnowflakeTarget
from bricksync.provider.target.aws import GlueTarget, AthenaTarget
from typing import List, Dict, Optional
import logging

logging.getLogger(__name__)

class BrickSync():
    def __init__(self, config: BrickSyncConfig):
        self.config = config
        self.secret_provider = self.config.secret_provider
        self.providers = self._initialize_providers()
        
    @classmethod
    def load(cls, config_path):
        config = BrickSyncConfig.load(config_path)
        return cls(config)
    
    @classmethod 
    def new(cls):
        return cls(BrickSyncConfig.new())
    
    def get_providers(self) -> Dict[str, Provider]:
        return self.providers
    
    def get_provider(self, provider_name: str) -> Provider:
        try:
            return self.get_providers()[provider_name]
        except Exception as e:
            raise Exception(f"Provider {provider_name} not found: {e}. You may need to add to config.")
        
    def add_sync(self, source: str, source_provider: str, target_provider: str, source_configuration = None):
        src = self.get_provider(source_provider)
        tgt = self.get_provider(target_provider)
        sync_conf = SyncConfig(source, source_provider, target_provider, source_configuration)
        self.config.add_sync(sync_conf)

    def show_syncs(self):
        return self.config.syncs
    
    def add_provider(self, name: str, provider: ProviderType, configuration=None):
        provider = ProviderConfig(provider, configuration=configuration)
        self.config.add_provider(name, provider)

    def _is_value_secret(self, value: str) -> bool:
        if value.startswith("secret://"):
            return True
        else:
            return False
        
    def _initialize_providers(self):
        logging.info("Initializing providers")
        providers_dct = {}
        for providers in self.config.providers:
            for k, v in providers.items():
                try: 
                  if v.provider.value == 'databricks' and v.type.value == 'source':
                      logging.info(f"Initializing databricks source provider {k}...")
                      providers_dct[k] = DatabricksSource.initialize(v,
                                                                    self.config.target_format_preference)
                  elif v.provider.value == 'snowflake' and v.type.value == 'target':
                      logging.info(f"Initializing snowflake target provider {k}...")
                      providers_dct[k] = SnowflakeTarget.initialize(v, self.config.target_sync_strategy)
                  elif v.provider.value == 'glue' and v.type.value == 'target':
                        logging.info(f"Initializing glue target provider {k}...")
                        providers_dct[k] = GlueTarget.initialize(v, self.config.target_sync_strategy)
                  elif v.provider.value == 'athena' and v.type.value == 'target':
                        logging.info(f"Initializing athena target provider {k}...")
                        providers_dct[k] = AthenaTarget.initialize(v, self.config.target_sync_strategy)    
                  else:
                      raise Exception(f"Unknown provider type {v.type}")
                except Exception as e:
                    raise Exception(f"Error initializing provider {k}: {e}")
        return providers_dct if len(providers_dct) > 0 else None
    
    def sync(self):
        logging.info("Starting sync")
        for sync_config in self.config.syncs:
            source_provider = self.get_provider(sync_config.source_provider)
            target_provider = self.get_provider(sync_config.target_provider)
            sources = source_provider.get_sources(sync_config)
            for source in sources:
                target = target_provider.get_target(source)
                logging.info(f"Syncing {source.table_name}")
                logging.info(target_provider.update_target(target))