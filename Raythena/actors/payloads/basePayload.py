from abc import ABC, abstractmethod
from typing import Union, Dict

from Raythena.actors.loggingActor import LoggingActor
from Raythena.utils.config import Config
from Raythena.utils.eventservice import PandaJob


class BasePayload(ABC):

    def __init__(self, id: str, logging_actor: LoggingActor,
                 config: Config) -> None:
        self.id = id
        self.logging_actor = logging_actor
        self.config = config

    @abstractmethod
    def start(self, job: PandaJob) -> None:
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def stop(self) -> None:
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def is_complete(self) -> bool:
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def return_code(self) -> int:
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def fetch_job_update(self) -> Union[None, Dict[str, str]]:
        raise NotImplementedError("Base method not implemented")
