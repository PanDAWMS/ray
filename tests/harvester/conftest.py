import os
import queue
import pytest
from raythena.drivers.communicators.harvesterFileMessenger import (
    HarvesterFileCommunicator,
)
from raythena.drivers.communicators.harvesterMock import HarvesterMock
from raythena.drivers.communicators.harvesterMock2205 import HarvesterMock2205


@pytest.fixture
def request_queue():
    return queue.Queue()


@pytest.fixture
def jobs_queue():
    return queue.Queue()


@pytest.fixture
def ranges_queue():
    return queue.Queue()


@pytest.fixture(params=[HarvesterMock, HarvesterMock2205])
def harvester_mock(request, config, request_queue, jobs_queue, ranges_queue):
    mock = request.param(request_queue, jobs_queue, ranges_queue, config)
    yield mock
    mock.stop()


def clean_files(files):
    for f in files:
        if os.path.isfile(f):
            os.remove(f)


@pytest.fixture
def harvester_file_communicator(tmpdir, config, request_queue, jobs_queue, ranges_queue):
    config.harvester["endpoint"] = str(tmpdir)
    communicator = HarvesterFileCommunicator(request_queue, jobs_queue, ranges_queue, config)
    yield communicator
    communicator.stop()
    clean_files(
        [
            communicator.jobrequestfile,
            communicator.jobspecfile,
            communicator.eventrequestfile,
            communicator.eventrangesfile,
        ]
    )
