import json
import os
import time
from Raythena.utils.eventservice import PandaJobRequest


class TestHarvesterFileMessenger:

    def check_job(self, jobs, sample_jobs):
        assert jobs is not None
        assert len(jobs) == len(sample_jobs)
        for sample_ID, jobID in zip(sample_jobs, jobs):
            assert sample_ID == jobID

    def test_get_job(self, harvester_file_communicator, sample_job, request_queue, jobs_queue):

        with open(harvester_file_communicator.jobspecfile, 'w') as f:
            json.dump(sample_job, f)

        harvester_file_communicator.start()
        request_queue.put(PandaJobRequest())
        job_communicator = jobs_queue.get(timeout=5)
        self.check_job(job_communicator, sample_job)
        os.remove(harvester_file_communicator.jobspecfile)

    def test_get_job_request(self, harvester_file_communicator, sample_job, request_queue, jobs_queue):
        harvester_file_communicator.start()
        request_queue.put(PandaJobRequest())

        while not os.path.exists(harvester_file_communicator.jobrequestfile):
            time.sleep(0.1)

        with open(harvester_file_communicator.jobspecfile, 'w') as f:
            json.dump(sample_job, f)
        jobs = jobs_queue.get(timeout=5)
        self.check_job(jobs, sample_job)

    def test_get_event_ranges(self, config, harvester_file_communicator):
        harvester_file_communicator.start()
        assert 1
