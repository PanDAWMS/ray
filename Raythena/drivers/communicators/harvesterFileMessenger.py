import os
import shutil
import time
import threading
import configparser
import json

from Raythena.utils.eventservice import EventRangeRequest, PandaJobRequest, PandaJobUpdate, EventRangeUpdate

from Raythena.drivers.communicators.baseCommunicator import BaseCommunicator


class HarvesterFileCommunicator(BaseCommunicator):

    def __init__(self, requestsQueue, jobQueue, eventRangesQueue, config):
        super().__init__(requestsQueue, jobQueue, eventRangesQueue, config)
        self.harvester_conf_file = os.path.expandvars(self.config.harvester['harvesterconf'])
        self.harvester_workdir = os.path.expandvars(self.config.harvester['endpoint'])
        if not os.path.isfile(self.harvester_conf_file):
            raise FileNotFoundError("Harvester config file not found")
        self.harvester_conf = configparser.ConfigParser()
        self.harvester_conf.read(self.harvester_conf_file)
        for k in self.harvester_conf['payload_interaction']:
            setattr(self, k, os.path.join(self.harvester_workdir, self.harvester_conf['payload_interaction'][k]))
        self.communicator_thread = threading.Thread(target=self.run, name="communicator-thread")

    def request_job(self, request):

        # Checks if a job file already exists
        if os.path.isfile(self.jobspecfile):
            with open(self.jobspecfile) as f:
                job = json.load(f)
                self.jobQueue.put(job)
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
            self.jobQueue.put(job)

    def request_event_ranges(self, request):
        if not os.path.isfile(self.eventrangesfile) and not os.path.exists(self.eventrequestfile):
            eventRequestFileTmp = f"{self.eventrequestfile}.tmp"
            with open(eventRequestFileTmp, 'w') as f:
                json.dump(request.request, f)
            shutil.move(eventRequestFileTmp, self.eventrequestfile)

        while not os.path.isfile(self.eventrangesfile):
            time.sleep(1)

        while os.path.isfile(self.eventrangesfile):
            try:
                with open(self.eventrangesfile, 'r') as f:
                    ranges = json.load(f)
                if os.path.isfile(self.eventrangesfile):
                    shutil.move(self.eventrangesfile, f"{self.eventrangesfile}-{self.ranges_requests_count}")
            except Exception:
                time.sleep(5)

        try:
            os.remove(self.eventrequestfile)
        except Exception:
            pass

        self.ranges_requests_count += 1
        self.eventRangesQueue.put(ranges)

    def update_job(self, request):
        pass

    def update_events(self, request):

        tmp_statusdumpfile = f"{self.eventstatusdumpjsonfile}.tmp"
        with open(tmp_statusdumpfile, 'w') as f:
            json.dump(request.range_update, f)
        while os.path.isfile(self.eventstatusdumpjsonfile):
            time.sleep(0.5)

        shutil.move(tmp_statusdumpfile, self.eventstatusdumpjsonfile)

    def run(self):
        while True:
            request = self.requestsQueue.get()
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

    def start(self):
        if not self.communicator_thread.is_alive():
            self.ranges_requests_count = 0
            self.communicator_thread.start()

    def stop(self):
        if self.communicator_thread.is_alive():
            self.requestsQueue.put(None)
            self.communicator_thread.join()
            self.communicator_thread = threading.Thread(target=self.run, name="communicator-thread")
