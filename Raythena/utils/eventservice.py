import json


class ESEncoder(json.JSONEncoder):

    def default(self, o):
        if isinstance(o, EventRange):
            return o.to_dict()
        if isinstance(o, EventRangeRequest):
            return o.request
        return super().default(o)


class EventRangeRequest:

    def __init__(self):
        self.request = dict()

    def __str__(self):
        return self.to_json_string()

    def add_event_request(self, pandaID, nRanges, taskId, jobsetID):
        self.request[pandaID] = {'pandaID': pandaID, 'nRanges': nRanges, 'taskId': taskId, 'jobsetID': jobsetID}

    def to_json_string(self):
        return json.dumps(self.request)

    @staticmethod
    def build_from_json_string(request_string):
        return EventRangeRequest.build_from_dict(json.loads(request_string))

    @staticmethod
    def build_from_dict(request_dict):
        request = EventRangeRequest()
        request.request.update(request_dict)
        return request


class EventRange:

    READY = 0
    ASSIGNED = 1
    DONE = 2
    STATES = [READY, ASSIGNED, DONE]

    def __init__(self, eventRangeID, startEvent, lastEvent, PFN, GUID, scope):
        self.lastEvent = lastEvent
        self.eventRangeID = eventRangeID
        self.startEvent = startEvent
        self.PFN = PFN
        self.GUID = GUID
        self.scope = scope
        self.status = EventRange.READY

    def set_assigned(self):
        self.status = EventRange.ASSIGNED

    def set_done(self):
        self.status = EventRange.DONE

    def __str__(self):
        return json.dumps(self.to_dict())

    def to_dict(self):
        return {
            'PFN': self.PFN,
            'lastEvent': self.lastEvent,
            'eventRangeID': self.eventRangeID,
            'startEvent': self.startEvent,
            'GUID': self.GUID
        }

    @staticmethod
    def build_from_json_string(eventRangeString):
        return EventRange.build_from_dict(json.loads(eventRangeString))

    @staticmethod
    def build_from_dict(eventRangeDict):
        return EventRange(eventRangeDict['eventRangeID'],
                          eventRangeDict['startEvent'],
                          eventRangeDict['lastEvent'],
                          eventRangeDict['PFN'],
                          eventRangeDict['GUID'],
                          eventRangeDict['scope'])
