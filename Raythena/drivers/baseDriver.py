from abc import ABC, abstractmethod


class BaseDriver(ABC):

    def __init__(self, config):
        self.config = config

    @abstractmethod
    def run(self):
        raise NotImplementedError("Base driver not implemented")

    @abstractmethod
    def stop(self):
        raise NotImplementedError("Base driver not implemented")
