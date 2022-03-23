from abc import abstractmethod
from typing import Union, Dict, List

from raythena.actors.payloads.basePayload import BasePayload
from raythena.utils.config import Config
from raythena.utils.eventservice import EventRange


class ESPayload(BasePayload):
    """
    Interface defining additional operations for payload handling event service jobs
    """

    def __init__(self, worker_id: str, config: Config):
        """
        Setup base payload attribute

        Args:
            worker_id: payload worker_id
            logging_actor: remote logger
            config: application config
        """
        super().__init__(worker_id, config)

    @abstractmethod
    def submit_new_ranges(self, event_ranges: Union[None, List[EventRange]]) -> None:
        """
        Submit a new list of event ranges to the payload. The event ranges should be saved until is can be processed

        Args:
            event_ranges: the event ranges list to process

        Returns:
            None
        """
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def fetch_ranges_update(self) -> Union[None, Dict[str, str]]:
        """
        Checks if event ranges update are available

        Returns:
            Dict holding event range update of processed events, None if no update is available
        """
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def should_request_more_ranges(self) -> bool:
        """
        Checks if the payload is ready to receive more event ranges

        Returns:
            True if the worker should send new event ranges to the payload, False otherwise
        """
        raise NotImplementedError("Base method not implemented")
