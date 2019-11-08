import json


class EventRangeRequest:

    def __init__(self):
        self.request = dict()

    def __str__(self):
        return self.to_json_string()

    def add_event_request(self, pandaId, nRanges, taskId, jobsetID):
        self.request[pandaId] = {'pandaId': pandaId, 'nRanges': nRanges, 'taskId': taskId, 'jobsetID': jobsetID}

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

    class Encoder(json.JSONEncoder):

        def default(self, o):
            if isinstance(o, EventRange):
                return o.to_json_string()
            return super().default(o)

    def __init__(self, eventRangeID, startEvent, lastEvent, LFN, GUID, scope):
        self.lastEvent = lastEvent
        self.eventRangeID = eventRangeID
        self.startEvent = startEvent
        self.LFN = LFN
        self.GUID = GUID
        self.scope = scope
        self.status = EventRange.READY

    def set_assigned(self):
        self.status = EventRange.ASSIGNED

    def set_done(self):
        self.status = EventRange.DONE

    def __str__(self):
        return self.to_json_string()

    def to_json_string(self):
        cp = self.__dict__.copy()
        cp.pop('status')
        return cp.__str__()

    @staticmethod
    def build_from_json_string(eventRangeString):
        return EventRange.build_from_dict(json.loads(eventRangeString))

    @staticmethod
    def build_from_dict(eventRangeDict):
        return EventRange(eventRangeDict['eventRangeID'],
                          eventRangeDict['startEvent'],
                          eventRangeDict['lastEvent'],
                          eventRangeDict['LFN'],
                          eventRangeDict['GUID'],
                          eventRangeDict['scope'])
