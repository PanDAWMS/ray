from abc import abstractmethod
from typing import Union, Dict

from Raythena.actors.loggingActor import LoggingActor
from Raythena.actors.payloads.basePayload import BasePayload
from Raythena.utils.config import Config
from Raythena.utils.eventservice import EventRange


class ESPayload(BasePayload):

    def __init__(self, id: str, logging_actor: LoggingActor, config: Config):
        super().__init__(id, logging_actor, config)

    @abstractmethod
    def submit_new_ranges(self, ranges: EventRange) -> None:
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def fetch_ranges_update(self) -> Union[None, Dict[str, str]]:
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def should_request_more_ranges(self) -> bool:
        raise NotImplementedError("Base method not implemented")
