from raythena.utils.eventservice import EventRangeRequest, PandaJobRequest


class TestHarvesterMock:
    def test_get_job(self, harvester_mock, request_queue, jobs_queue):
        harvester_mock.start()
        request_queue.put(PandaJobRequest())
        job = jobs_queue.get(timeout=5)
        assert job is not None and isinstance(job, dict)

    def test_get_ranges(
        self, harvester_mock, request_queue, jobs_queue, ranges_queue
    ):
        harvester_mock.start()
        request_queue.put(PandaJobRequest())
        jobs = jobs_queue.get(timeout=5)

        n_events = harvester_mock.nevents
        evnt_request = EventRangeRequest()
        for pandaID, job in jobs.items():
            evnt_request.add_event_request(
                pandaID, n_events, job["taskID"], job["jobsetID"]
            )
        request_queue.put(evnt_request)
        ranges = ranges_queue.get(timeout=5)
        assert ranges is not None
        assert isinstance(ranges, dict)
        for _pandaID, job_ranges in ranges.items():
            assert len(job_ranges) == n_events

        # should return 0 ranges per job
        request_queue.put(evnt_request)
        ranges = ranges_queue.get(timeout=5)
        assert ranges is not None
        assert isinstance(ranges, dict)
        for _pandaID, job_ranges in ranges.items():
            assert len(job_ranges) == 0
