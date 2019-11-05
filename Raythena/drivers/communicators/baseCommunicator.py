
from abc import ABC, abstractmethod


class BaseCommunicator(ABC):

    def __init__(self, config):
        self.config = config

    @abstractmethod
    def get_event_ranges(self, evnt_request):
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def get_job(self, job_request):
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def update_job(self, job_status):
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def update_events(self, evnt_status):
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def get_panda_queue_name(self):
        raise NotImplementedError("Base method not implemented")
