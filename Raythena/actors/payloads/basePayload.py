
from abc import ABC, abstractmethod


class BasePayload(ABC):

    def __init__(self, id, logging_actor, config):
        self.id = id
        self.logging_actor = logging_actor
        self.config = config

    @abstractmethod
    def start(self, job):
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def stop(self):
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def is_complete(self):
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def returncode(self):
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def fetch_job_update(self):
        raise NotImplementedError("Base method not implemented")

