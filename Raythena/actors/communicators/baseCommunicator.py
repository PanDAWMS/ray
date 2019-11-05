
from abc import ABC, abstractmethod


class BaseCommunicator(ABC):

    def __init__(self, actor, config):
        self.actor = actor
        self.config = config

    @abstractmethod
    def start(self):
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def stop(self):
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def fix_command(self, command):
        raise NotImplementedError("Base method not implemented")
