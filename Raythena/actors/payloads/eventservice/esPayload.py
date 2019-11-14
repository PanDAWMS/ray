from abc import abstractmethod

from Raythena.actors.payloads.basePayload import BasePayload


class ESPayload(BasePayload):

    def __init__(self, id, logging_actor, config):
        super().__init__(id, logging_actor, config)

    @abstractmethod
    def submit_new_ranges(self, ranges):
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def fetch_ranges_update(self):
        raise NotImplementedError("Base method not implemented")

    @abstractmethod
    def should_request_more_ranges(self):
        raise NotImplementedError("Base method not implemented")
