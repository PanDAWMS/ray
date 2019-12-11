
import queue
import pytest
import os
from Raythena.drivers.communicators.harvesterMock import HarvesterMock
from Raythena.drivers.communicators.harvesterFileMessenger import HarvesterFileCommunicator
from Raythena.utils.eventservice import PandaJobRequest


@pytest.fixture
def request_queue():
    return queue.Queue()


@pytest.fixture
def jobs_queue():
    return queue.Queue()


@pytest.fixture
def ranges_queue():
    return queue.Queue()


@pytest.fixture
def harvester_mock(config, request_queue, jobs_queue, ranges_queue):
    mock = HarvesterMock(request_queue, jobs_queue, ranges_queue, config)
    yield mock
    mock.stop()


def clean_files(files):
    for f in files:
        if os.path.isfile(f):
            os.remove(f)


@pytest.fixture
def harvester_file_communicator(tmpdir, config, request_queue, jobs_queue, ranges_queue):
    config.harvester['endpoint'] = tmpdir
    communicator = HarvesterFileCommunicator(request_queue, jobs_queue, ranges_queue, config)
    yield communicator
    communicator.stop()
    clean_files([communicator.jobrequestfile, communicator.jobspecfile, communicator.eventrequestfile, communicator.eventrangesfile])


@pytest.fixture
def sample_job(harvester_mock, request_queue, jobs_queue):
    harvester_mock.start()
    request_queue.put(PandaJobRequest())
    job = jobs_queue.get(timeout=5)
    harvester_mock.stop()
    return job
