
from abc import ABC, abstractmethod


class BasePayload(ABC):

    def __init__(self, actor, config):
        self.actor = actor
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
    def submit_new_ranges(self, ranges):
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def fetch_job_update(self):
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def fetch_ranges_update(self):
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def should_request_more_ranges(self):
        raise NotImplementedError("Base method not implemented")
