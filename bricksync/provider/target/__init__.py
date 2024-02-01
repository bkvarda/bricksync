from abc import ABC, ABCMeta, abstractmethod
from bricksync.provider import Provider
from sqlglot.dialects.dialect import Dialect

class TargetProvider(Provider):
    @abstractmethod
    def get_target(self):
        pass
    @abstractmethod
    def update_target(self):
        pass
    @abstractmethod
    def convert_view_dialect(self, view_definition: str, source_dialect: Dialect):
        pass