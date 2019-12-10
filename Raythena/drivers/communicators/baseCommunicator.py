
from abc import ABC, abstractmethod


class BaseCommunicator(ABC):

    def __init__(self, requestsQueue, jobQueue, eventRangesQueue, config):
        self.requestsQueue = requestsQueue
        self.jobQueue = jobQueue
        self.eventRangesQueue = eventRangesQueue
        self.config = config

    @abstractmethod
    def start(self):
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def stop(self):
        raise NotImplementedError("Base method not implemented")
