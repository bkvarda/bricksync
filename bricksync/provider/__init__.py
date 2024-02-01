from abc import ABCMeta, abstractmethod 
from bricksync.config import ProviderConfig


class Provider(metaclass=ABCMeta):
    @classmethod
    @abstractmethod
    def initialize(cls, config: ProviderConfig):
        pass
    @abstractmethod
    def authenticate():
        pass