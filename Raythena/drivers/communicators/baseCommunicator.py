
from abc import ABC, abstractmethod
from queue import Queue
from Raythena.utils.config import Config


class BaseCommunicator(ABC):

    def __init__(self, requests_queue: Queue, job_queue: Queue, event_ranges_queue: Queue, config: Config) -> None:
        self.requests_queue = requests_queue
        self.job_queue = job_queue
        self.event_ranges_queue = event_ranges_queue
        self.config = config

    @abstractmethod
    def start(self) -> None:
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def stop(self) -> None:
        raise NotImplementedError("Base method not implemented")
