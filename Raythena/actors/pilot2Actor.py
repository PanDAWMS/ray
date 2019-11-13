import ray
import os
import asyncio
import uvloop
from asyncio.subprocess import PIPE, create_subprocess_shell
from Raythena.utils.ray import get_node_ip
from Raythena.utils.eventservice import EventRangeRequest, Messages
from Raythena.utils.importUtils import import_from_string
@ray.remote
class Pilot2Actor:

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
        self.eventranges = dict()
        self.command_hook = list()
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        asyncio.get_event_loop()
        self.node_ip = get_node_ip()
        self.pilot_process = None
        self.state = Pilot2Actor.READY_FOR_JOB
        cwd = os.getcwd()
        self.pilot_dir = os.path.expandvars(self.config.pilot.get('workdir', cwd))
        if os.path.isdir(self.pilot_dir):
            os.chdir(self.pilot_dir)

        communicator = self.config.pilot['communicator']
        self.communicator_class = import_from_string(f"Raythena.actors.communicators.{communicator}")
        self.communicator = self.communicator_class(self, self.config)
        self.communicator.start()
        self.logging_actor.info.remote(self.id, "Ray worker started")

    def stagein(self):
        cwd = os.getcwd()
        self.pilot_job_dir = os.path.join(self.pilot_dir, self.job['PandaID'])
        if not os.path.isdir(self.pilot_job_dir):
            self.logging_actor.warn.remote(self.id, f"Specified path {self.pilot_job_dir} does not exist. Using cwd {cwd}")

        subdir = f"{self.id}_{os.getpid()}"
        self.pilot_process_dir = os.path.join(self.pilot_job_dir, subdir)
        os.mkdir(self.pilot_process_dir)

        input_files = self.job['inFiles'].split(",")
        for input_file in input_files:
            in_abs = input_file if os.path.isabs(input_file) else os.path.join(self.pilot_dir, input_file)
            if os.path.isfile(in_abs):
                basename = os.path.basename(in_abs)
                staged_file = os.path.join(self.pilot_process_dir, basename)
                os.symlink(in_abs, staged_file)

        os.chdir(self.pilot_process_dir)

        command = self.build_pilot_command()
        self.logging_actor.info.remote(self.id, f"Final payload command: {command}")
        self.pilot_process = self.asyncio_run_coroutine(create_subprocess_shell, command, stdout=PIPE, stderr=PIPE, executable='/bin/bash')
        self.logging_actor.info.remote(self.id, f"Started subprocess {self.pilot_process.pid}")
        self.transition_state(Pilot2Actor.READY_FOR_EVENTS)
    
    def stageout(self):
        self.logging_actor.info.remote(self.id, "Performing stageout")
        #TODO
        self.transition_state(Pilot2Actor.FINISHING)
        self.terminate_actor()
    
    def transition_state(self, dest):
        if dest not in self.TRANSITIONS[self.state]:
            self.logging_actor.error.remote(self.id, f"Illegal transition from {self.state} to {dest}")
            raise Exception(f"Illegal state transition for actor {self.id}")
        self.state = dest

    def receive_job(self, reply, job):
        self.job = job
        if reply == Messages.REPLY_OK and self.job:
            self.transition_state(Pilot2Actor.STAGEIN)
            self.stagein()
        else:
            self.transition_state(Pilot2Actor.DONE)
            self.logging_actor.error.remote(self.id, f"Could not fetch job. Set state to done.")

        return self.return_message('received_job')

    def receive_event_ranges(self, reply, eventranges_update):
        if reply == Messages.REPLY_NO_MORE_EVENT_RANGES or not eventranges_update:
            #no new ranges... finish processing local cache then terminate actor
            self.transition_state(Pilot2Actor.FINISHING_LOCAL_RANGES)
            return
        self.transition_state(Pilot2Actor.PROCESSING)
        for pandaid, job_ranges in eventranges_update.items():
            if pandaid not in self.eventranges.keys():
                self.eventranges[pandaid] = list()
            self.eventranges[pandaid] += job_ranges
        self.logging_actor.debug.remote(self.id, f"Received {len(job_ranges)} eventRanges")
        return self.return_message('received_event_range')

    def get_ranges(self, req: EventRangeRequest):
        """
        Called by the communicator plugin in order to fetch event ranges from local cache
        """
        self.logging_actor.info.remote(self.id, f"Received  event range request from pilot: {req.to_json_string()}")
        if len(req.request) > 1:
            self.logging_actor.warn.remote(self.id, f"Pilot request ranges for more than one panda job. Serving only the first job")

        for pandaID, pandaID_request in req.request.items():
            ranges = self.eventranges.get(pandaID, None)
            # if we're not in a state where were supposed finish local cache, wait until a getrange request is sent to the driver
            while self.state != Pilot2Actor.FINISHING_LOCAL_RANGES and not ranges:
                self.async_sleep(1)

            if not ranges: # --> implies that state == FINISHING_LOCAL_RANGES and empty local cache
                return list()
            
            nranges = min(len(ranges), int(pandaID_request['nRanges']))
            res, self.eventranges[pandaID] = ranges[:nranges], ranges[(nranges):]
            self.logging_actor.info.remote(self.id, f"Served {nranges} eventranges. Remaining on {len(self.eventranges[pandaID])}")
            self.should_request_ranges()
            return res

    def asyncio_run_coroutine(self, coroutine, *args, **kwargs):
        return asyncio.get_event_loop().run_until_complete(coroutine(*args, **kwargs))

    def register_command_hook(self, func):
        self.command_hook.append(func)

    def build_pilot_command(self):
        """
        """
        cmd = str()
        conda_activate = os.path.join(self.config.resources['condabindir'], 'activate')
        pilot_venv = self.config.pilot['virtualenv']
        if os.path.isfile(conda_activate) and pilot_venv is not None:
            cmd += f"source {conda_activate} {pilot_venv}; source /cvmfs/atlas.cern.ch/repo/sw/local/setup-yampl.sh;"
        prodSourceLabel = self.job['prodSourceLabel']

        # temporary hack to fix a bug in Athena 22.0 release
        cmd += "export LD_LIBRARY_PATH=$SCRATCH/raythena/lib:$LD_LIBRARY_PATH; "

        pilot_bin = os.path.join(self.config.pilot['bindir'], "pilot.py")
        # use exec to replace the shell process with python. Allows to send signal to the python process if needed
        cmd += f"exec python {pilot_bin} -q {self.panda_queue} -r {self.panda_queue} -s {self.panda_queue} " \
               f"-i PR -j {prodSourceLabel} --pilot-user=ATLAS -t -w generic --url=http://127.0.0.1 " \
               f"-p 8080 -d --allow-same-user=False --resource-type MCORE;"

        for f in self.command_hook:
            cmd = f(cmd)

        return cmd

    def return_message(self, message, data=None):
        return self.id, message, data
    
    def interrupt(self):
        """
        Interruption from driver
        """
        self.logging_actor.warn.remote(self.id, "Received interruption from driver")

    def terminate_actor(self):
        self.logging_actor.info.remote(self.id, f"stopping actor")
        pexit = self.pilot_process.returncode
        self.logging_actor.debug.remote(self.id, f"Pilot2 return code: {pexit}")
        if pexit is None:
            self.pilot_process.terminate()

        stdout, stderr = self.asyncio_run_coroutine(self.pilot_process.communicate)

        self.communicator.stop()
        self.transition_state(Pilot2Actor.DONE)

    def async_sleep(self, delay):
        self.asyncio_run_coroutine(asyncio.sleep, delay)

    def should_request_ranges(self):
        # do not transition if not in a state allowing for event ranges request
        if Pilot2Actor.READY_FOR_EVENTS not in self.TRANSITIONS[self.state]:
            return False

        if not self.eventranges:
            self.transition_state(Pilot2Actor.READY_FOR_EVENTS)
            return True

        for ranges in self.eventranges.values():
            if len(ranges) < self.config.resources['corepernode'] * 2:
                self.transition_state(Pilot2Actor.READY_FOR_EVENTS)
                return True
        return False

    def get_message(self):
        """
        Return a message to the driver depending on the current actor state
        """
        #while True:
        if self.state == Pilot2Actor.READY_FOR_JOB:
            # ready to get a new job
            self.transition_state(Pilot2Actor.JOB_REQUESTED)
            return self.return_message(Messages.REQUEST_NEW_JOB)
        elif self.pilot_process is not None and self.pilot_process.returncode is not None:
            # pilot process ended... Start stageout
            # if an exception occurs when changing state, this means that pilot ended early
            # send final job / event update
            self.logging_actor.info.remote(self.id, f"Pilot ended with return code {self.pilot_process.returncode}")
            self.transition_state(Pilot2Actor.STAGEOUT)
            self.stageout()
            return self.return_message(Messages.PROCESS_DONE)
        elif self.state == Pilot2Actor.READY_FOR_EVENTS:
            req = EventRangeRequest()
            req.add_event_request(self.job['PandaID'],
                                  self.config.resources['corepernode'] * 4,
                                  self.job['taskID'],
                                  self.job['jobsetID'])
            self.transition_state(Pilot2Actor.EVENT_RANGES_REQUESTED)
            return self.return_message(Messages.REQUEST_EVENT_RANGES, req.to_json_string())
        else: #TODO process jobupdate / eventupdate received from pilot
            self.async_sleep(1)
        
        return self.return_message(Messages.IDLE)
