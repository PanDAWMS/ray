from abc import ABC, abstractmethod
from typing import Any, Optional
from raythena.utils.config import Config
from raythena.utils.eventservice import PandaJob


class BasePayload(ABC):
    """
    Defines the interface that should be implemented by payload plugins. Payload plugins receive a
    panda job specification and are responsible to handle the execution of the
    """

    def __init__(self, worker_id: str, config: Config) -> None:
        """
        Setup base payload attributes

        Args:
            worker_id: payload worker_id
            logging_actor: remote logger
            config: application config
        """
        self.worker_id = worker_id
        self.config = config

    @abstractmethod
    def start(self, job: PandaJob) -> None:
        """
        This function should start the payload process to process the provided job.
        The payload should be started in a separate thread or process and should not block until it is complete

        Args:
            job: panda job specification to be processed by the payload

        Returns:
            None
        """
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def stagein(self) -> None:
        """
        Called after the worker performed stage-int of input files. Allows implementation of payload-specific stage-in

        Returns:
            None
        """
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def stageout(self) -> None:
        """
        Called after the worker performed stage-out of output files. Allows implementation of payload-specific stage-out

        Returns:
            None
        """
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def stop(self) -> None:
        """
        Notify the payload that it should stop processing the current job

        Returns:
            None
        """
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def is_complete(self) -> bool:
        """
        Checks if the payload ended

        Returns:
            True if the payload finished processing the current job
        """
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def return_code(self) -> int:
        """
        Retrieve the return code of the payload indicating whether or not the execution was successful

        Returns:
            payload return code
        """
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def fetch_job_update(self) -> Optional[dict[str, Any]]:
        """
        Tries to get a job update from the payload

        Returns:
            None if no job update is available or a dict holding the update
        """
        raise NotImplementedError("Base method not implemented")
