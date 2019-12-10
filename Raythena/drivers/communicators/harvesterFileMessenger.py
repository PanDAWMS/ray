import os
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
            raise Exception("Harvester config file not found")
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
                os.rename(request_tmp, self.jobrequestfile)

            # wait on job file creation
            while not os.path.isfile(self.jobspecfile):
                time.sleep(1)

            # load job and remove request file
            with open(self.jobspecfile) as f:
                job = json.load(f)

            if os.path.isfile(self.jobrequestfile):
                os.remove(self.jobrequestfile)
            self.jobQueue.put(job)

    def request_event_ranges(self, request):
        if not os.path.exists(self.eventrequestfile):
            eventRequestFileTmp = f"{self.eventrequestfile}.tmp"
            with open(eventRequestFileTmp, 'w') as f:
                json.dump(request.request, f)
            os.rename(eventRequestFileTmp, self.eventrequestfile)

            while not os.path.isfile(self.eventrangesfile):
                time.sleep(1)

            with open(self.eventrangesfile) as f:
                ranges = json.load(f)

            if os.path.isfile(self.eventrequestfile):
                os.remove(self.eventrequestfile)
            self.eventRangesQueue.put(ranges)

    def update_job(self, request):
        pass

    def update_events(self, request):
        pass

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
        self.communicator_thread.start()

    def stop(self):
        self.requestsQueue.put(None)
        self.communicator_thread.join()
