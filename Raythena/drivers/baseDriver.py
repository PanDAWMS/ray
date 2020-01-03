from abc import ABC, abstractmethod
from Raythena.utils.config import Config


class BaseDriver(ABC):

    def __init__(self, config: Config) -> None:
        self.config = config

    @abstractmethod
    def run(self) -> None:
        raise NotImplementedError("Base driver not implemented")

    @abstractmethod
    def stop(self) -> None:
        raise NotImplementedError("Base driver not implemented")
