import os
import time

import pytest
import requests
from raythena.actors.payloads.eventservice.pilothttp import PilotHttpPayload
from raythena.utils.eventservice import EventRange, PandaJob


class MockPopen:

    def __init__(self, returncode):
        self.returncode = returncode

    def poll(self):
        return self.returncode

    def wait(self):
        while self.returncode is None:
            time.sleep(1)
        return self.returncode

    def terminate(self):
        self.returncode = 0


class MockPayload(PilotHttpPayload):

    def _start_payload(self):
        self.pilot_process = MockPopen(None)


@pytest.mark.usefixtures("requires_ray")
class TestPilotHttp:

    def wait_server_start(self):
        while True:
            try:
                requests.post('http://127.0.0.1:8080')
            except requests.exceptions.ConnectionError:
                time.sleep(0.5)
            else:
                break

    def setup_payload(self, config):
        return MockPayload("a1", config)

    @pytest.fixture
    def payload(self, tmpdir, config, sample_job):
        cwd = os.getcwd()
        config.ray['workdir'] = str(tmpdir)
        os.chdir(tmpdir)
        job_dict = list(sample_job.values())[0]
        job = PandaJob(job_dict)
        payload = self.setup_payload(config)
        payload.start(job)
        self.wait_server_start()
        yield payload
        payload.stop()
        os.chdir(cwd)

    def test_getjob(self, payload, is_eventservice, config, sample_job):
        if not is_eventservice:
            pytest.skip()
        job_dict = list(sample_job.values())[0]
        job = PandaJob(job_dict)
        res = requests.post('http://127.0.0.1:8080/server/panda/getJob').json()
        assert job['PandaID'] == PandaJob(res)['PandaID']

        assert requests.post(
            'http://127.0.0.1:8080/unknown').json()['StatusCode'] == 500

        payload.stop()
        assert payload.is_complete()
        assert payload.return_code() == payload.pilot_process.returncode

    def endpoint_not_implemented(self, endpoint):
        assert requests.post(f'http://127.0.0.1:8080/server/panda/{endpoint}').json()['StatusCode'] == 500

    @pytest.mark.usefixtures("payload")
    def test_updateJobsInBulk(self):
        self.endpoint_not_implemented("updateJobsInBulk")

    @pytest.mark.usefixtures("payload")
    def test_getStatus(self):
        self.endpoint_not_implemented("getStatus")

    @pytest.mark.usefixtures("payload")
    def test_getKeyPair(self):
        self.endpoint_not_implemented("getKeyPair")

    def test_jobUpdate(self, payload, config, is_eventservice):
        if not is_eventservice:
            pytest.skip()

        assert not payload.fetch_job_update()
        data = {"pilotErrorCode": '0'}
        res = requests.post('http://127.0.0.1:8080/server/panda/updateJob',
                            data=data).json()
        assert res['StatusCode'] == 0
        # Disabled as job update are currently not forwarded to the driver
        # job_update = payload.fetch_job_update()
        # assert job_update['pilotErrorCode'][0] == data['pilotErrorCode']

    def test_rangesUpdate(self, payload, config, is_eventservice, sample_job,
                          sample_ranges, nevents):
        if not is_eventservice:
            pytest.skip()

        assert not payload.fetch_ranges_update()
        data = {"pilotErrorCode": 0}
        res = requests.post(
            'http://127.0.0.1:8080/server/panda/updateEventRanges',
            data=data).json()
        assert res['StatusCode'] == 0

    def test_getranges(self, payload, config, is_eventservice, sample_job,
                       sample_ranges, nevents):
        if not is_eventservice:
            pytest.skip()

        job_dict = list(sample_job.values())[0]
        job = PandaJob(job_dict)

        data = {
            "pandaID": job["PandaID"],
            "nRanges": nevents,
            "jobsetID": job["jobsetID"],
            "taskID": job["taskID"]
        }
        res = requests.post(
            'http://127.0.0.1:8080/server/panda/getEventRanges').json()
        assert res['StatusCode'] == 500
        assert payload.should_request_more_ranges()
        ranges = list()
        for r in list(sample_ranges.values())[0]:
            ranges.append(EventRange.build_from_dict(r))
        payload.submit_new_ranges(ranges)
        payload.submit_new_ranges(None)

        res = requests.post('http://127.0.0.1:8080/server/panda/getEventRanges',
                            data=data).json()
        assert res['StatusCode'] == 0
        assert len(res['eventRanges']) == nevents

        res = requests.post('http://127.0.0.1:8080/server/panda/getEventRanges',
                            data=data).json()
        assert res['StatusCode'] == 0
        assert len(res['eventRanges']) == 0
        assert not payload.should_request_more_ranges()
        data["pandaID"] = "None"
        assert requests.post(
            'http://127.0.0.1:8080/server/panda/getEventRanges',
            data=data).json()['StatusCode'] == -1
