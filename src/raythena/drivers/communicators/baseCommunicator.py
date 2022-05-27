from abc import ABC, abstractmethod
from queue import Queue
from typing import Optional, Mapping, Sequence

from raythena.utils.config import Config
from raythena.utils.eventservice import EventRangeRequest, PandaJobRequest, PandaJobUpdate, EventRangeUpdate, \
    JobReport, EventRangeDef, JobDef

RequestData = Optional[PandaJobRequest, EventRangeUpdate, JobReport, EventRangeRequest, PandaJobUpdate]


class BaseCommunicator(ABC):
    """
    Base communicator class, used mostly for documentation purposes. Defines methods that are necessary
    to be implemented by different communicators as well as setting up queues used to communicate with other threads.
    """

    def __init__(self, requests_queue: Queue[RequestData], job_queue: Queue[Mapping[str, JobDef]],
                 event_ranges_queue: Queue[Mapping[str, Sequence[EventRangeDef]]], config: Config) -> None:
        """
        Base constructor setting up queues and application config

        Args:
            requests_queue: communication queue where requests are
            job_queue: queue used to place jobs retrieved by the communicator
            event_ranges_queue: queue used to place event ranges retrieved by the communicator
            config: app config
        """
        self.requests_queue: Queue[RequestData] = requests_queue
        self.job_queue: Queue[Mapping[str, JobDef]] = job_queue
        self.event_ranges_queue: Queue[Mapping[str, Sequence[EventRangeDef]]] = event_ranges_queue
        self.config = config

    @abstractmethod
    def start(self) -> None:
        """
        Base method to start communicator

        Returns:
            None
        """
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def stop(self) -> None:
        """
        Base method to stop communicator

        Returns:
            None
        """
        raise NotImplementedError("Base method not implemented")
