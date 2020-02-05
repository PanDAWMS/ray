from typing import Union, Dict, List

from raythena.actors.payloads.eventservice.esPayload import ESPayload
from raythena.utils.eventservice import PandaJob, EventRange


class Pilot2SFM(ESPayload):

    def submit_new_ranges(self, event_ranges: Union[None, List[EventRange]]) -> None:
        pass

    def fetch_ranges_update(self) -> Union[None, Dict[str, str]]:
        pass

    def should_request_more_ranges(self) -> bool:
        pass

    def start(self, job: PandaJob) -> None:
        pass

    def stagein(self) -> None:
        pass

    def stageout(self) -> None:
        pass

    def stop(self) -> None:
        pass

    def is_complete(self) -> bool:
        pass

    def return_code(self) -> int:
        pass

    def fetch_job_update(self) -> Union[None, Dict[str, str]]:
        pass