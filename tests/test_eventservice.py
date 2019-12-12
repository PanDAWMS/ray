import pytest
from Raythena.utils.eventservice import EventRange, EventRangeQueue, EventRangeRequest, EventRangeUpdate
from Raythena.utils.eventservice import PandaJob, PandaJobQueue, PandaJobRequest, PandaJobUpdate


class TestEventRangeRequest:

    def test_from_dict_init(self):
        request_dict = {
            "0": {
                "nRanges": 10,
                "pandaID": "0",
                "taskID": "0",
                "jobsetID": "0"
            },
            "1": {
                "nRanges": 20,
                "pandaID": "1",
                "taskID": "1",
                "jobsetID": "1"
            }
        }

        ranges_request = EventRangeRequest.build_from_dict(request_dict)
        ranges_request_init = EventRangeRequest()
        for pandaID, req in request_dict.items():
            ranges_request_init.add_event_request(pandaID, req['nRanges'], req['taskID'], req['jobsetID'])
        assert len(request_dict) == len(ranges_request) == len(ranges_request_init)
        for id1, id2, id3 in zip(ranges_request, ranges_request_init, request_dict):
            assert ranges_request[id1]['pandaID'] == ranges_request_init[id2]['pandaID'] == request_dict[id3]['pandaID']


class TestEventRangeUpdate:

    def test_build_range_update(self, nevents, sample_rangeupdate, sample_failed_rangeupdate):
        pandaID = "0"
        ranges_update = EventRangeUpdate.build_from_dict(pandaID, sample_rangeupdate)
        assert pandaID in ranges_update
        ranges = ranges_update[pandaID]
        assert len(ranges) == nevents
        for r in ranges:
            assert "eventRangeID" in r and "eventStatus" in r and "path" in r and "type" in r and "chksum" in r and "fsize" in r and "guid" in r

        ranges_update = EventRangeUpdate.build_from_dict(pandaID, sample_failed_rangeupdate)
        assert pandaID in ranges_update
        ranges = ranges_update[pandaID]
        assert len(ranges) == nevents
        for r in ranges:
            assert "eventRangeID" in r and "eventStatus" in r and \
                "path" not in r and "type" not in r and "chksum" not in r and "fsize" not in r and "guid" not in r


class TestEventRangeQueue:

    def test_new(self, nevents, sample_job, sample_ranges):
        ranges_queue = EventRangeQueue()
        assert not ranges_queue.no_more_ranges
        assert len(ranges_queue) == 0
        ranges = list(sample_ranges.values())[0]
        ranges_queue = EventRangeQueue.build_from_list(ranges)
        assert len(ranges) == len(ranges_queue) == ranges_queue.nranges_available() == ranges_queue.nranges_remaining() == nevents
        assert ranges_queue.nranges_assigned() == ranges_queue.nranges_done() == ranges_queue.nranges_failed() == 0

    def test_concat(self, nevents, sample_job, sample_ranges):
        ranges_queue = EventRangeQueue()
        ranges = list(sample_ranges.values())[0]
        ranges_queue.concat(ranges)
        assert len(ranges) == len(ranges_queue) == ranges_queue.nranges_available() == ranges_queue.nranges_remaining() == nevents
        assert ranges_queue.nranges_assigned() == ranges_queue.nranges_done() == ranges_queue.nranges_failed() == 0
        assert ranges_queue[ranges[0]['eventRangeID']].eventRangeID == ranges[0]['eventRangeID']
        for r in ranges:
            assert r['eventRangeID'] in ranges_queue

    def test_update(self, sample_job, sample_ranges, nevents, sample_rangeupdate, sample_failed_rangeupdate):
        pandaID = "0"
        ranges = list(sample_ranges.values())[0]
        ranges_queue = EventRangeQueue.build_from_list(ranges)

        nsuccess = int(nevents / 2)
        ranges_update = EventRangeUpdate.build_from_dict(pandaID, sample_rangeupdate)[pandaID][:nsuccess]
        failed_ranges_update = EventRangeUpdate.build_from_dict(pandaID, sample_failed_rangeupdate)[pandaID][nsuccess:]

        with pytest.raises(Exception):
            ranges_queue.update_ranges(ranges_update)

        ranges_queue.get_next_ranges(nevents)
        ranges_queue.update_ranges(ranges_update)
        assert len(ranges_update) == ranges_queue.nranges_done()
        assert ranges_queue.nranges_available() == 0
        assert ranges_queue.nranges_assigned() == nevents - len(ranges_update)
        assert ranges_queue.nranges_remaining() == nevents - len(ranges_update)

        ranges_queue.update_ranges(failed_ranges_update)
        assert len(failed_ranges_update) == ranges_queue.nranges_failed()
        assert ranges_queue.nranges_assigned() == 0
        assert ranges_queue.nranges_remaining() == 0

    def test_get_next(self, sample_job, sample_ranges):
        ranges_queue = EventRangeQueue()
        assert not ranges_queue.get_next_ranges(10)
        ranges = list(sample_ranges.values())[0]
        ranges_queue.concat(ranges)
        nranges = len(ranges_queue)
        nranges_requested = max(1, int(nranges / 3))
        requested_ranges = ranges_queue.get_next_ranges(nranges_requested)
        assert len(requested_ranges) == nranges_requested
        assert ranges_queue.nranges_assigned() == nranges_requested
        assert ranges_queue.nranges_remaining() == nranges
        assert ranges_queue.nranges_available() == nranges - nranges_requested
        for requested_range in requested_ranges:
            assert ranges_queue[requested_range.eventRangeID].status == EventRange.ASSIGNED

        requested_ranges = ranges_queue.get_next_ranges(nranges)
        assert len(requested_ranges) == nranges - nranges_requested
        assert ranges_queue.nranges_available() == 0
        assert ranges_queue.nranges_assigned() == ranges_queue.nranges_remaining() == nranges
        assert len(ranges_queue.get_next_ranges(1)) == 0


class TestEventRanges:

    def test_new(self):
        id = "Range-0"
        start = 0
        last = 0
        guid = "abc"
        pfn = "/path/to/file"
        scope = "13Tev"

        r = EventRange(id, start, last, pfn, guid, scope)
        assert r.status == EventRange.READY
        assert r.nevents() == 1

    def test_build_from_dict(self):
        id = "Range-0"
        start = 0
        last = 0
        guid = "abc"
        pfn = "/path/to/file"
        scope = "13Tev"
        r_dict = {
            "eventRangeID": id,
            "LFN": pfn,
            "lastEvent": last,
            "startEvent": start,
            "GUID": guid,
            "scope": scope
        }
        range_from_dict = EventRange.build_from_dict(r_dict)
        assert range_from_dict.PFN == pfn and range_from_dict.eventRangeID == id and range_from_dict.startEvent == start \
            and range_from_dict.lastEvent == last and range_from_dict.GUID == guid and range_from_dict.scope == scope
        assert range_from_dict.status == EventRange.READY


class TestPandaJobQueue:

    def test_build_pandajob_queue(self, njobs, sample_multijobs):
        assert len(sample_multijobs) == njobs
        pandajob_queue = PandaJobQueue()
        assert len(pandajob_queue) == 0


class TestPandaJob:

    def test_build_pandajob(self, sample_job):
        job_dict = list(sample_job.values())[0]
        job = PandaJob(job_dict)
        for k in job_dict:
            assert k in job
            assert job_dict[k] == job[k]


class TestPandaJobRequest:

    def test_build_pandajob_request(self):
        request_dict = {
            "node": "nodename",
            "diskSpace": 230000,
            "workingGroup": "grp",
            "prodSourceLabel": "test",
            "computingElement": "ce",
            "siteName": "nersc",
            "resourceType": "rt",
            "mem": 230000,
            "cpu": 32,
            "allowOtherCountry": "false"
        }
        jobrequest = PandaJobRequest(**request_dict)
        for k in request_dict:
            assert request_dict[k] == getattr(jobrequest, k)


class TestPandaJobUpdate:

    def test_build_pandajob_update(self):
        update_dict = {
            'node': ['nid00038'],
            'startTime': ['1574112042.86'],
            'jobMetrics': ['coreCount=32'],
            'siteName': ['NERSC_Cori_p2_ES'],
            'timestamp': ['2019-11-18T13:20:45-08:00'],
            'coreCount': ['32'],
            'attemptNr': ['0'],
            'jobId': ['7a75654803d17d54f9129e2a6974beda'],
            'batchID': ['25932742'],
            'state': ['starting'],
            'schedulerID': ['unknown'],
            'pilotID': ['unknown|SLURM|PR|2.2.2 (1)']
        }
        jobupdate = PandaJobUpdate(**update_dict)
        for k in update_dict:
            assert update_dict[k] == getattr(jobupdate, k)
