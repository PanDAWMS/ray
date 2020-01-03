import os
import shutil
import time
import configparser
import json
from queue import Queue
from Raythena.utils.config import Config
from Raythena.utils.eventservice import EventRangeRequest, PandaJobRequest, PandaJobUpdate, EventRangeUpdate
from Raythena.utils.exception import ExThread
from Raythena.drivers.communicators.baseCommunicator import BaseCommunicator


class HarvesterFileCommunicator(BaseCommunicator):

    def __init__(self, requests_queue: Queue, job_queue: Queue,
                 event_ranges_queue: Queue, config: Config) -> None:
        super().__init__(requests_queue, job_queue, event_ranges_queue, config)
        self.harvester_workdir = os.path.expandvars(
            self.config.harvester['endpoint'])
        self.ranges_requests_count = 0
        self._parse_harvester_config()
        self.communicator_thread = ExThread(target=self.run,
                                            name="communicator-thread")

    def _parse_harvester_config(self) -> None:
        self.harvester_conf_file = os.path.expandvars(
            self.config.harvester['harvesterconf'])
        if not os.path.isfile(self.harvester_conf_file):
            raise FileNotFoundError("Harvester config file not found")
        self.harvester_conf = configparser.ConfigParser()
        self.harvester_conf.read(self.harvester_conf_file)
        for k in self.harvester_conf['payload_interaction']:
            setattr(
                self, k,
                os.path.join(self.harvester_workdir,
                             self.harvester_conf['payload_interaction'][k]))
        if not hasattr(self, "jobspecfile"):
            self.jobspecfile = str()
        if not hasattr(self, "jobrequestfile"):
            self.jobrequestfile = str()
        if not hasattr(self, "eventrangesfile"):
            self.eventrangesfile = str()
        if not hasattr(self, "eventrequestfile"):
            self.eventrequestfile = str()
        if not hasattr(self, "eventstatusdumpjsonfile"):
            self.eventstatusdumpjsonfile = str()

    def request_job(self, request: PandaJobRequest) -> None:

        # Checks if a job file already exists
        if os.path.isfile(self.jobspecfile):
            with open(self.jobspecfile) as f:
                job = json.load(f)
                self.job_queue.put(job)
        else:
            # create request file if necessary
            if not os.path.isfile(self.jobrequestfile):
                request_tmp = f"{self.jobrequestfile}.tmp"
                with open(request_tmp, 'w') as f:
                    json.dump(request.to_dict(), f)
                shutil.move(request_tmp, self.jobrequestfile)

            # wait on job file creation
            while not os.path.isfile(self.jobspecfile):
                time.sleep(0.01)

            # load job and remove request file
            with open(self.jobspecfile) as f:
                job = json.load(f)

        try:
            os.remove(self.jobrequestfile)
        except FileNotFoundError:
            pass
        try:
            os.remove(self.jobspecfile)
        except FileNotFoundError:
            pass
        if job:
            self.job_queue.put(job)

    def request_event_ranges(self, request: EventRangeRequest) -> None:
        if not os.path.isfile(self.eventrangesfile) and not os.path.exists(
                self.eventrequestfile):
            event_request_file_tmp = f"{self.eventrequestfile}.tmp"
            with open(event_request_file_tmp, 'w') as f:
                json.dump(request.request, f)
            shutil.move(event_request_file_tmp, self.eventrequestfile)

        while not os.path.isfile(self.eventrangesfile):
            time.sleep(1)

        while os.path.isfile(self.eventrangesfile):
            try:
                with open(self.eventrangesfile, 'r') as f:
                    ranges = json.load(f)
                if os.path.isfile(self.eventrangesfile):
                    shutil.move(
                        self.eventrangesfile,
                        f"{self.eventrangesfile}-{self.ranges_requests_count}")
            except Exception:
                time.sleep(5)

        try:
            os.remove(self.eventrequestfile)
        except Exception:
            pass

        self.ranges_requests_count += 1
        self.event_ranges_queue.put(ranges)

    def update_job(self, request: PandaJobUpdate) -> None:
        pass

    def update_events(self, request: EventRangeUpdate) -> None:

        tmp_status_dump_file = f"{self.eventstatusdumpjsonfile}.tmp"
        with open(tmp_status_dump_file, 'w') as f:
            json.dump(request.range_update, f)
        while os.path.isfile(self.eventstatusdumpjsonfile):
            time.sleep(0.5)

        shutil.move(tmp_status_dump_file, self.eventstatusdumpjsonfile)

    def run(self) -> None:
        while True:
            request = self.requests_queue.get()
            if isinstance(request, PandaJobRequest):
                self.request_job(request)
            elif isinstance(request, EventRangeRequest):
                self.request_event_ranges(request)
            elif isinstance(request, PandaJobUpdate):
                self.update_job(request)
            elif isinstance(request, EventRangeUpdate):
                self.update_events(request)
            else:  # if any other request is received, stop the thread
                break

    def start(self) -> None:
        if not self.communicator_thread.is_alive():
            self.communicator_thread.start()

    def stop(self) -> None:
        if self.communicator_thread.is_alive():
            self.requests_queue.put(None)
            self.communicator_thread.join()
            self.communicator_thread = ExThread(target=self.run,
                                                name="communicator-thread")
