
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
    def submit_new_job(self, job):
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def submit_new_ranges(self, pandaID, ranges):
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def fetch_job_update(self):
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def fetch_ranges_update(self):
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def should_request_more_ranges(self, pandaID):
        raise NotImplementedError("Base method not implemented")
