import os
import time
import shutil
import ray
import json

from Raythena.utils.eventservice import EventRangeRequest, Messages, EventRangeUpdate
from Raythena.utils.plugins import PluginsRegistry
from Raythena.utils.ray import get_node_ip
from Raythena.utils.exception import IllegalWorkerState, StageInFailed


@ray.remote(num_cpus=0)
class ESWorker:
    """
    Actor running on HPC compute node. Each actor will start a payload plugin
    """

    READY_FOR_JOB = 0  # initial state, before the first job request
    JOB_REQUESTED = 1  # job has been requested to the driver, waiting for result
    READY_FOR_EVENTS = 2  # ready to request new events for the current job
    EVENT_RANGES_REQUESTED = 3  # event ranges have been requested to the driver, waiting for result
    FINISHING_LOCAL_RANGES = 4  # same as PROCESSING, except that no more event ranges are available, will move to STAGEOUT once local cache is empty
    PROCESSING = 5  # currently processing event ranges
    FINISHING = 6  # Performing cleanup of resources, preparing final server update
    DONE = 7  # Actor has finished processing job
    STAGEIN = 8  # Staging-in data.
    STAGEOUT = 9  # Staging-out data

    STATES_NAME = {
        READY_FOR_JOB: "READY_FOR_JOB",
        JOB_REQUESTED: "JOB_REQUESTED",
        READY_FOR_EVENTS: "READY_FOR_EVENTS",
        EVENT_RANGES_REQUESTED: "EVENT_RANGES_REQUESTED",
        FINISHING_LOCAL_RANGES: "FINISHING_LOCAL_RANGES",
        PROCESSING: "PROCESSING",
        FINISHING: "FINISHING",
        DONE: "DONE",
        STAGEIN: "STAGEIN",
        STAGEOUT: "STAGEOUT"
    }

    # authorize state transition from x to y if y in TRANSITION[X]
    TRANSITIONS_EVENTSERVICE = {
        READY_FOR_JOB: [JOB_REQUESTED],
        JOB_REQUESTED: [STAGEIN, DONE],
        STAGEIN: [READY_FOR_EVENTS],
        READY_FOR_EVENTS: [EVENT_RANGES_REQUESTED],
        EVENT_RANGES_REQUESTED: [FINISHING_LOCAL_RANGES, PROCESSING],
        FINISHING_LOCAL_RANGES: [STAGEOUT],
        PROCESSING: [READY_FOR_EVENTS],
        STAGEOUT: [FINISHING],
        FINISHING: [DONE],
        DONE: [READY_FOR_JOB]
    }

    TRANSITIONS_STANDARD = {
        READY_FOR_JOB: [JOB_REQUESTED],
        JOB_REQUESTED: [STAGEIN, DONE],
        STAGEIN: [PROCESSING],
        PROCESSING: [STAGEOUT],
        STAGEOUT: [FINISHING],
        FINISHING: [DONE],
        DONE: [READY_FOR_JOB]
    }

    def __init__(self, actor_id, config, logging_actor):
        self.id = actor_id
        self.config = config
        self.logging_actor = logging_actor
        self.job = None
        self.transitions = ESWorker.TRANSITIONS_STANDARD
        self.node_ip = get_node_ip()
        self.state = ESWorker.READY_FOR_JOB
        self.workdir = os.path.expandvars(self.config.ray.get('workdir', os.getcwd()))
        if not os.path.isdir(self.workdir):
            self.workdir = os.getcwd()
        self.plugin_registry = PluginsRegistry()
        payload = self.config.payload['plugin']
        self.payload_class = self.plugin_registry.get_plugin(payload)
        self.payload = self.payload_class(self.id, self.logging_actor, self.config)
        self.logging_actor.info.remote(self.id, "Ray worker started")

    def stagein(self):

        self.payload_job_dir = os.path.join(self.workdir, self.job['PandaID'])
        if not os.path.isdir(self.payload_job_dir):
            self.logging_actor.warn.remote(self.id, f"Specified path {self.payload_job_dir} does not exist. Using cwd {os.getcwd()}")
            self.payload_job_dir = self.workdir

        subdir = f"{self.id}_{os.getpid()}"
        self.payload_actor_process_dir = os.path.join(self.payload_job_dir, subdir)
        try:
            os.mkdir(self.payload_actor_process_dir)
            os.chdir(self.payload_actor_process_dir)
        except Exception:
            raise StageInFailed(self.id)

        input_files = self.job['inFiles'].split(",")
        for input_file in input_files:
            in_abs = input_file if os.path.isabs(input_file) else os.path.join(self.workdir, input_file)
            if os.path.isfile(in_abs):
                basename = os.path.basename(in_abs)
                staged_file = os.path.join(self.payload_actor_process_dir, basename)
                os.symlink(in_abs, staged_file)

        self.payload.start(self.job)
        self.transition_state(ESWorker.READY_FOR_EVENTS if self.is_event_service_job() else ESWorker.PROCESSING)

    def stageout(self):
        self.logging_actor.info.remote(self.id, "Performing stageout")
        # TODO move payload out file to harvester dir, drain jobupdate and rangeupdate from payload
        self.transition_state(ESWorker.FINISHING)
        self.terminate_actor()

    def transition_state(self, dest):
        if dest not in self.transitions[self.state]:
            self.logging_actor.error.remote(self.id, f"Illegal transition from {ESWorker.STATES_NAME[self.state]} to {ESWorker.STATES_NAME[dest]}")
            raise IllegalWorkerState(id=self.id, src_state=ESWorker.STATES_NAME[self.state], dst_state=ESWorker.STATES_NAME[dest])
        self.state = dest

    def is_event_service_job(self):
        return self.job and self.job['eventService']

    def set_transitions(self):
        if self.is_event_service_job():
            self.transitions = ESWorker.TRANSITIONS_EVENTSERVICE
        else:
            self.transitions = ESWorker.TRANSITIONS_STANDARD

    def receive_job(self, reply, job):
        self.job = job
        if reply == Messages.REPLY_OK and self.job:
            self.transition_state(ESWorker.STAGEIN)
            self.set_transitions()
            self.stagein()
        else:
            self.transition_state(ESWorker.DONE)
            self.logging_actor.error.remote(self.id, f"Could not fetch job. Set state to done.")

        return self.return_message('received_job')

    def mark_new_job(self):
        """
        Mark the worker as ready for new jobs
        """
        self.transition_state(ESWorker.READY_FOR_JOB)

    def receive_event_ranges(self, reply, eventranges_update):
        if reply == Messages.REPLY_NO_MORE_EVENT_RANGES or not eventranges_update:
            # no new ranges... finish processing local cache then terminate actor
            self.transition_state(ESWorker.FINISHING_LOCAL_RANGES)
            self.payload.submit_new_ranges(None)
            return
        self.transition_state(ESWorker.PROCESSING)
        for crange in eventranges_update:
            if not os.path.isabs(crange.PFN):
                crange.PFN = os.path.join(os.path.expandvars(self.config.harvester['endpoint']), crange.PFN)
            self.payload.submit_new_ranges(crange)
        self.logging_actor.debug.remote(self.id, f"Received {len(eventranges_update)} eventRanges")
        return self.return_message('received_event_range')

    def return_message(self, message, data=None):
        return self.id, message, data

    def interrupt(self):
        """
        Interruption from driver
        """
        self.logging_actor.warn.remote(self.id, "Received interruption from driver")

    def terminate_actor(self):
        self.logging_actor.info.remote(self.id, f"stopping actor")
        self.payload.stop()
        self.transition_state(ESWorker.DONE)

    def should_request_ranges(self):
        # do not transition if not in a state allowing for event ranges request
        if ESWorker.READY_FOR_EVENTS not in self.transitions[self.state]:
            return False

        res = self.payload.should_request_more_ranges()
        if res:
            self.transition_state(ESWorker.READY_FOR_EVENTS)
        return res

    def move_ranges_file(self, ranges_update):
        harvester_endpoint = self.config.harvester.get("endpoint", "")
        if not os.path.isdir(harvester_endpoint):
            return
        ranges = json.loads(ranges_update['eventRanges'][0])
        ranges = EventRangeUpdate.build_from_dict(self.job.get_id(), ranges)
        for range_update in ranges[self.job.get_id()]:
            cfile = range_update.get("path", None)
            if cfile and os.path.isfile(cfile):
                dst = os.path.join(harvester_endpoint, os.path.basename(cfile) if os.path.isabs(cfile) else cfile)
                range_update["path"] = dst
                self.logging_actor.debug.remote(self.id, f"Moving {cfile} to {dst}")
                shutil.move(cfile, dst)
        return ranges

    def get_message(self):
        """
        Return a message to the driver depending on the current actor state
        """
        while self.state != ESWorker.DONE:
            if self.state == ESWorker.READY_FOR_JOB:
                # ready to get a new job
                self.transition_state(ESWorker.JOB_REQUESTED)
                return self.return_message(Messages.REQUEST_NEW_JOB)
            elif self.payload.is_complete():
                # payload process ended... Start stageout
                # if an exception occurs when changing state, this means that the payload ended early
                # send final job / event update
                self.logging_actor.info.remote(self.id, f"Payload ended with return code {self.payload.returncode()}")
                self.transition_state(ESWorker.STAGEOUT)
                self.stageout()
                return self.return_message(Messages.PROCESS_DONE)
            elif self.is_event_service_job() and (self.state == ESWorker.READY_FOR_EVENTS or self.should_request_ranges()):
                req = EventRangeRequest()
                req.add_event_request(self.job['PandaID'],
                                      self.config.resources['corepernode'] * 2,
                                      self.job['taskID'],
                                      self.job['jobsetID'])
                self.transition_state(ESWorker.EVENT_RANGES_REQUESTED)
                return self.return_message(Messages.REQUEST_EVENT_RANGES, req)
            elif self.state == ESWorker.DONE:
                return self.return_message(Messages.PROCESS_DONE)
            else:
                job_update = self.payload.fetch_job_update()
                if job_update:
                    self.logging_actor.info.remote(self.id, f"Fetched jobupdate from payload: {job_update}")
                    return self.return_message(Messages.UPDATE_JOB, job_update)

                ranges_update = self.payload.fetch_ranges_update()
                if ranges_update:
                    self.logging_actor.info.remote(self.id, f"Fetched rangesupdate from payload: {ranges_update}")
                    ranges_update = self.move_ranges_file(ranges_update)
                    return self.return_message(Messages.UPDATE_EVENT_RANGES, ranges_update)

                time.sleep(1)  # Nothing to do, sleeping...

        return self.return_message(Messages.IDLE)
