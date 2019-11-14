import os
import time

import ray

from Raythena.utils.eventservice import EventRangeRequest, Messages
from Raythena.utils.importUtils import import_from_string
from Raythena.utils.ray import get_node_ip


@ray.remote
class ESWorker:
    """
    Actor running on HPC compute node. Each actor will start a payload plugin 
    """

    READY_FOR_JOB=0 # initial state, before the first job request
    JOB_REQUESTED=1 # job has been requested to the driver, waiting for result
    READY_FOR_EVENTS=2 # ready to request new events for the current job
    EVENT_RANGES_REQUESTED=3 # event ranges have been requested to the driver, waiting for result
    FINISHING_LOCAL_RANGES=4 # same as PROCESSING, except that no more event ranges are available, will move to STAGEOUT once local cache is empty
    PROCESSING=5 # currently processing event ranges
    FINISHING=6 # Performing cleanup of resources, preparing final server update
    DONE=7 # Actor has finished processing job
    STAGEIN=8 # Staging-in data.
    STAGEOUT=9 # Staging-out data

    # authorize state transition from x to y if y in TRANSITION[X]
    TRANSITIONS = {
        READY_FOR_JOB: [JOB_REQUESTED],
        JOB_REQUESTED: [STAGEIN, DONE],
        READY_FOR_EVENTS: [EVENT_RANGES_REQUESTED],
        EVENT_RANGES_REQUESTED: [FINISHING_LOCAL_RANGES, PROCESSING],
        FINISHING_LOCAL_RANGES: [STAGEOUT],
        PROCESSING: [READY_FOR_EVENTS],
        FINISHING: [DONE],
        DONE: [READY_FOR_JOB],
        STAGEIN: [READY_FOR_EVENTS],
        STAGEOUT: [FINISHING]
        }

    def __init__(self, actor_id, panda_queue, config, logging_actor):
        self.id = actor_id
        self.config = config
        self.panda_queue = panda_queue
        self.logging_actor = logging_actor
        self.job = None
        self.node_ip = get_node_ip()
        self.state = ESWorker.READY_FOR_JOB
        cwd = os.getcwd()
        self.workdir = os.path.expandvars(self.config.ray.get('workdir', cwd))
        if os.path.isdir(self.workdir):
            os.chdir(self.workdir)

        payload = self.config.payload['plugin']
        self.payload_class = import_from_string(f"Raythena.actors.payloads.eventservice.{payload}")
        self.payload = self.payload_class(self.id, self.logging_actor, self.config)
        self.logging_actor.info.remote(self.id, "Ray worker started")

    def stagein(self):
        cwd = os.getcwd()
        self.payload_job_dir = os.path.join(self.workdir, self.job['PandaID'])
        if not os.path.isdir(self.payload_job_dir):
            self.logging_actor.warn.remote(self.id, f"Specified path {self.payload_job_dir} does not exist. Using cwd {cwd}")

        subdir = f"{self.id}_{os.getpid()}"
        self.payload_actor_process_dir = os.path.join(self.payload_job_dir, subdir)
        os.mkdir(self.payload_actor_process_dir)

        input_files = self.job['inFiles'].split(",")
        for input_file in input_files:
            in_abs = input_file if os.path.isabs(input_file) else os.path.join(self.workdir, input_file)
            if os.path.isfile(in_abs):
                basename = os.path.basename(in_abs)
                staged_file = os.path.join(self.payload_actor_process_dir, basename)
                os.symlink(in_abs, staged_file)

        os.chdir(self.payload_actor_process_dir)

        self.payload.start(self.job)
        self.transition_state(ESWorker.READY_FOR_EVENTS)
    
    def stageout(self):
        self.logging_actor.info.remote(self.id, "Performing stageout")
        #TODO move payload out file to harvester dir, drain jobupdate and rangeupdate from payload
        self.transition_state(ESWorker.FINISHING)
        self.terminate_actor()
    
    def transition_state(self, dest):
        if dest not in self.TRANSITIONS[self.state]:
            self.logging_actor.error.remote(self.id, f"Illegal transition from {self.state} to {dest}")
            #TODO Handle illegal state transition accordingly
            raise Exception(f"Illegal state transition for actor {self.id}")
        self.state = dest

    def receive_job(self, reply, job):
        self.job = job
        if reply == Messages.REPLY_OK and self.job:
            self.transition_state(ESWorker.STAGEIN)
            self.stagein()
        else:
            self.transition_state(ESWorker.DONE)
            self.logging_actor.error.remote(self.id, f"Could not fetch job. Set state to done.")

        return self.return_message('received_job')

    def receive_event_ranges(self, reply, eventranges_update):
        if reply == Messages.REPLY_NO_MORE_EVENT_RANGES or not eventranges_update:
            #no new ranges... finish processing local cache then terminate actor
            self.transition_state(ESWorker.FINISHING_LOCAL_RANGES)
            self.payload.submit_new_ranges(None)
            return
        self.transition_state(ESWorker.PROCESSING)
        for pandaid, job_ranges in eventranges_update.items():
            for crange in job_ranges:
                self.payload.submit_new_ranges(crange)
        self.logging_actor.debug.remote(self.id, f"Received {len(job_ranges)} eventRanges")
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
        if ESWorker.READY_FOR_EVENTS not in self.TRANSITIONS[self.state]:
            return False

        res = self.payload.should_request_more_ranges()
        if res:
            self.transition_state(ESWorker.READY_FOR_EVENTS)
        return res

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
            elif self.state == ESWorker.READY_FOR_EVENTS or self.should_request_ranges():
                req = EventRangeRequest()
                req.add_event_request(self.job['PandaID'],
                                    self.config.resources['corepernode'] * 4,
                                    self.job['taskID'],
                                    self.job['jobsetID'])
                self.transition_state(ESWorker.EVENT_RANGES_REQUESTED)
                return self.return_message(Messages.REQUEST_EVENT_RANGES, req.to_json_string())
            else:
                job_update = self.payload.fetch_job_update()
                if job_update:
                    self.logging_actor.info.remote(self.id, f"Fetched jobupdate from payload: {job_update}")
                    return self.return_message(Messages.UPDATE_JOB, job_update)
                
                ranges_update = self.payload.fetch_ranges_update()
                if ranges_update:
                    self.logging_actor.info.remote(self.id, f"Fetched rangesupdate from payload: {ranges_update}")
                    return self.return_message(Messages.UPDATE_EVENT_RANGES, ranges_update)

                time.sleep(1) # Nothing to do, sleeping...
        
        return self.return_message(Messages.IDLE)
