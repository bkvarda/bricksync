
from bricksync.config import BrickSyncConfig, ProviderType, ProviderConfig, SyncConfig
from bricksync.provider import Provider
from bricksync.table import Table, View
from bricksync.provider.catalog import CatalogProvider
from bricksync.provider.catalog.databricks import DatabricksCatalog
from bricksync.provider.catalog.snowflake import SnowflakeCatalog
from bricksync.provider.catalog.glue import GlueCatalog
from typing import List, Dict, Optional, Union, Tuple
import logging

logging.getLogger(__name__)

class BrickSync():
    def __init__(self, config: BrickSyncConfig):
        self.config = config
        self.initialized = {}
        self.providers = {}
        
        if len(self.config.providers) >= 1:
            self._initialize_providers()
        
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
        sync_conf = SyncConfig(source, source_provider, target_provider, source_configuration)
        self.config.add_sync(sync_conf)

    def show_syncs(self):
        return self.config.syncs
    
    def add_provider(self, name: str, provider: ProviderType, configuration=None, lazy_init=False):
        provider = ProviderConfig(provider, configuration=configuration)
        self.config.add_provider(name, provider)
        if lazy_init:
            return
        
        return self._initialize_provider(name, provider)

    def _sync(self, source_provider: CatalogProvider, src: Union[Table, View],
              target_provider: CatalogProvider, target: str, options: Optional[Dict[str, str]] = None):
        target_catalog = target_provider.get_catalog_from_name(src)
        target_schema = target_provider.get_schema_from_name(src)
        target_provider.create_catalog(target_catalog)
        target_provider.create_schema(target_catalog, target_schema)
        if src.is_view():
            base_tables = src.base_tables
            for t in base_tables:
                self._sync(source_provider, t, target_provider, target)
            target_provider.create_or_refresh_view(src)
        else:
            # if delta table
            if src.is_delta():
                try:
                    iceberg = src.to_iceberg_table()
                    target_provider.create_or_refresh_external_table(iceberg)
                except:
                    raise Exception("Error converting delta table to iceberg")
            elif src.is_iceberg():
                target_provider.create_or_refresh_external_table(src)
            else:
                raise Exception("Unsupported table type")
        return
 
    
    def sync(self, source_provider: str, source: str, 
             target_provider: str, target: str, **kwargs):
        src_provider: CatalogProvider = self.get_provider(source_provider)
        tgt_provider: CatalogProvider = self.get_provider(target_provider)
        source_table: Union[View, Table] = src_provider.get_table(source)
        self._sync(src_provider, source_table, tgt_provider, target)
        return
    
    def sync_all(self, source_provider: str, source: str, target_providers: List[str], target: str, **kwargs):
        src_provider: CatalogProvider = self.get_provider(source_provider)
        for tgt in target_providers:
            tgt_provider: CatalogProvider = self.get_provider(tgt)
            self._sync(src_provider, source, tgt_provider, target)
        return

    def _is_value_secret(self, value: str) -> bool:
        if value.startswith("secret://"):
            return True
        else:
            return False
        
    def _initialize_provider(self, name: str, provider_conf: ProviderConfig):
 
        if self.initialized.get(name):
            return
        
        k = name
        v = provider_conf
        if provider_conf.provider.value == 'databricks':
            logging.info(f"Initializing databricks provider {k}...")
            self.providers[k] = DatabricksCatalog.initialize(v)
        elif v.provider.value == 'snowflake':
            logging.info(f"Initializing snowflake provider {k}...")
            self.providers[k] = SnowflakeCatalog.initialize(v)
        elif v.provider.value == 'glue':
            logging.info(f"Initializing glue provider {k}...")
            self.providers[k] = GlueCatalog.initialize(v)
        else:
            raise Exception(f"Unknown provider type {v.type}")

        self.initialized[name] = True
        return self

    def _initialize_providers(self):
        logging.info("Initializing providers")
        for providers in self.config.providers:
            for k, v in providers.items():
                if k in self.initialized:
                    continue
                self._initialize_provider(k, v)
        return self
    
    def initialize(self):
        return self._initialize_providers()

            

            
            