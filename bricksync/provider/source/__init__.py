from abc import ABC, ABCMeta, abstractmethod
from bricksync.provider import Provider
from bricksync.table import Source

class SourceProvider(Provider):
    @abstractmethod
    def get_sources(self) -> Source:
        pass