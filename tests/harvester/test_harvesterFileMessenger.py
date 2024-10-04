import json
import os
import time

from raythena.utils.eventservice import EventRangeRequest, PandaJobRequest


class TestHarvesterFileMessenger:
    def check_job(self, jobs, sample_jobs):
        assert jobs is not None
        assert len(jobs) == len(sample_jobs)
        for sample_ID, jobID in zip(sample_jobs, jobs):
            assert sample_ID == jobID

    def test_get_job(
        self, harvester_file_communicator, sample_job, request_queue, jobs_queue
    ):
        with open(harvester_file_communicator.jobspecfile, "w") as f:
            json.dump(sample_job, f)

        harvester_file_communicator.start()
        request_queue.put(PandaJobRequest())
        job_communicator = jobs_queue.get(timeout=5)
        self.check_job(job_communicator, sample_job)

    def test_get_job_request(
        self, harvester_file_communicator, sample_job, request_queue, jobs_queue
    ):
        harvester_file_communicator.start()
        request_queue.put(PandaJobRequest())

        while not os.path.exists(harvester_file_communicator.jobrequestfile):
            time.sleep(0.1)

        with open(harvester_file_communicator.jobspecfile, "w") as f:
            json.dump(sample_job, f)
        jobs = jobs_queue.get(timeout=5)
        self.check_job(jobs, sample_job)

    def test_restart(self, harvester_file_communicator):
        ref_thread = harvester_file_communicator.communicator_thread
        assert not harvester_file_communicator.communicator_thread.is_alive()
        harvester_file_communicator.stop()
        assert not harvester_file_communicator.communicator_thread.is_alive()
        harvester_file_communicator.start()
        assert harvester_file_communicator.communicator_thread.is_alive()
        assert ref_thread == harvester_file_communicator.communicator_thread
        harvester_file_communicator.stop()
        assert not harvester_file_communicator.communicator_thread.is_alive()
        assert ref_thread != harvester_file_communicator.communicator_thread
        harvester_file_communicator.start()
        ref_thread = harvester_file_communicator.communicator_thread
        assert harvester_file_communicator.communicator_thread.is_alive()
        harvester_file_communicator.start()
        assert harvester_file_communicator.communicator_thread.is_alive()
        assert harvester_file_communicator.communicator_thread == ref_thread

    def test_get_event_ranges(
        self,
        config,
        harvester_file_communicator,
        request_queue,
        ranges_queue,
        sample_job,
    ):
        harvester_file_communicator.start()

        n_events = 3
        evnt_request = EventRangeRequest()
        for pandaID, job in sample_job.items():
            evnt_request.add_event_request(
                pandaID, n_events, job["taskID"], job["jobsetID"]
            )
        request_queue.put(evnt_request)

        while not os.path.isfile(harvester_file_communicator.eventrequestfile):
            time.sleep(0.01)

        ranges_res = {}
        with open(harvester_file_communicator.eventrequestfile) as f:
            communicator_request = json.load(f)
            for pandaIDSent, pandaIDCom in zip(
                evnt_request, communicator_request
            ):
                assert pandaIDSent == pandaIDCom
                assert (
                    evnt_request[pandaIDSent]["nRanges"]
                    == communicator_request[pandaIDSent]["nRanges"]
                )
                ranges_res[pandaIDSent] = [
                    {
                        "lastEvent": 0,
                        "eventRangeID": "0",
                        "startEvent": 0,
                        "scope": "scope_value",
                        "LFN": "/path/to/file",
                        "GUID": "worker_id",
                    }
                ] * n_events
        with open(harvester_file_communicator.eventrangesfile, "w") as f:
            json.dump(ranges_res, f)
        ranges_com = ranges_queue.get(timeout=5)

        for pandaIDSent, pandaIDCom in zip(ranges_res, ranges_com):
            assert pandaIDSent == pandaIDCom
            assert (
                len(ranges_res[pandaIDSent])
                == len(ranges_com[pandaIDSent])
                == n_events
            )

        assert not os.path.isfile(harvester_file_communicator.eventrequestfile)
        assert not os.path.isfile(harvester_file_communicator.eventrangesfile)
        request_queue.put(evnt_request)
        while not os.path.isfile(harvester_file_communicator.eventrequestfile):
            time.sleep(0.01)

        ranges_res = {}
        for pandaID in evnt_request:
            ranges_res[pandaID] = []
        with open(harvester_file_communicator.eventrangesfile, "w") as f:
            json.dump(ranges_res, f)
        ranges_com = ranges_queue.get(timeout=5)
        for pandaIDSent, pandaIDCom in zip(ranges_res, ranges_com):
            assert pandaIDSent == pandaIDCom
            assert (
                len(ranges_res[pandaIDSent])
                == len(ranges_com[pandaIDSent])
                == 0
            )
