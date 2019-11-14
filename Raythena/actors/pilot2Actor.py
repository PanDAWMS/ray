import ray
import os
import time
import threading
from queue import Queue
from subprocess import Popen, DEVNULL
from Raythena.utils.ray import get_node_ip
from Raythena.utils.eventservice import EventRangeRequest, Messages
from Raythena.utils.importUtils import import_from_string


@ray.remote
class Pilot2Actor:
    """
    Actor running on HPC compute node. Each actor will start a pilot2 process 
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
        self.max_job = 1 # hardcoded max job has raythena is not designed to run multiple jobs. Move in config if necessary
        self.njobs = 0
        self.command_hook = list()
        self.node_ip = get_node_ip()
        self.pilot_process = None
        self.state = Pilot2Actor.READY_FOR_JOB
        cwd = os.getcwd()
        self.pilot_dir = os.path.expandvars(self.config.pilot.get('workdir', cwd))
        if os.path.isdir(self.pilot_dir):
            os.chdir(self.pilot_dir)

        self.updateEventRangesQueue = Queue()

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
        # using PIPE will cause the subprocess to hang because
        # we're not reading data using communicate() and the pipe buffer becomes full as pilot2
        # generates a lot of data to the stdout pipe
        # see https://docs.python.org/3.7/library/subprocess.html#subprocess.Popen.wait
        self.pilot_process = Popen(command, stdin=DEVNULL, stdout=DEVNULL, stderr=DEVNULL, shell=True, close_fds=True)
        self.logging_actor.info.remote(self.id, f"Started subprocess {self.pilot_process.pid}")
        self.transition_state(Pilot2Actor.READY_FOR_EVENTS)
    
    def stageout(self):
        self.logging_actor.info.remote(self.id, "Performing stageout")
        #TODO move payload out file to harvester dir, drain jobupdate and rangeupdate from communicator
        self.transition_state(Pilot2Actor.FINISHING)
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
            self.transition_state(Pilot2Actor.STAGEIN)
            self.communicator.submit_new_job(job)
            self.njobs += 1
            if self.njobs >= self.max_job:
                self.communicator.submit_new_job(None)
            self.stagein()
        else:
            self.transition_state(Pilot2Actor.DONE)
            self.logging_actor.error.remote(self.id, f"Could not fetch job. Set state to done.")

        return self.return_message('received_job')

    def receive_event_ranges(self, reply, eventranges_update):
        if reply == Messages.REPLY_NO_MORE_EVENT_RANGES or not eventranges_update:
            #no new ranges... finish processing local cache then terminate actor
            self.transition_state(Pilot2Actor.FINISHING_LOCAL_RANGES)
            self.communicator.submit_new_ranges(self.job['PandaID'], None)
            return
        self.transition_state(Pilot2Actor.PROCESSING)
        for pandaid, job_ranges in eventranges_update.items():
            for crange in job_ranges:
                self.communicator.submit_new_ranges(pandaid, crange)
        self.logging_actor.debug.remote(self.id, f"Received {len(job_ranges)} eventRanges")
        return self.return_message('received_event_range')

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
        pexit = self.pilot_process.poll()
        self.logging_actor.debug.remote(self.id, f"Pilot2 return code: {pexit}")
        if pexit is None:
            self.pilot_process.terminate()
            pexit = self.pilot_process.wait()

        self.communicator.stop()
        self.transition_state(Pilot2Actor.DONE)

    def should_request_ranges(self):
        # do not transition if not in a state allowing for event ranges request
        if Pilot2Actor.READY_FOR_EVENTS not in self.TRANSITIONS[self.state]:
            return False

        res = self.communicator.should_request_more_ranges(self.job['PandaID'])
        if res:
            self.transition_state(Pilot2Actor.READY_FOR_EVENTS)
        return res

    def get_message(self):
        """
        Return a message to the driver depending on the current actor state
        """
        while self.state != Pilot2Actor.DONE:
            if self.state == Pilot2Actor.READY_FOR_JOB:
                # ready to get a new job
                self.transition_state(Pilot2Actor.JOB_REQUESTED)
                return self.return_message(Messages.REQUEST_NEW_JOB)
            elif self.pilot_process is not None and self.pilot_process.poll() is not None:
                # pilot process ended... Start stageout
                # if an exception occurs when changing state, this means that pilot ended early
                # send final job / event update
                self.logging_actor.info.remote(self.id, f"Pilot ended with return code {self.pilot_process.returncode}")
                self.transition_state(Pilot2Actor.STAGEOUT)
                self.stageout()
                return self.return_message(Messages.PROCESS_DONE)
            elif self.state == Pilot2Actor.READY_FOR_EVENTS or self.should_request_ranges():
                req = EventRangeRequest()
                req.add_event_request(self.job['PandaID'],
                                    self.config.resources['corepernode'] * 4,
                                    self.job['taskID'],
                                    self.job['jobsetID'])
                self.transition_state(Pilot2Actor.EVENT_RANGES_REQUESTED)
                return self.return_message(Messages.REQUEST_EVENT_RANGES, req.to_json_string())
            else:
                job_update = self.communicator.fetch_job_update()
                if job_update:
                    self.logging_actor.info.remote(self.id, f"Fetched jobupdate from communicator: {job_update}")
                    return self.return_message(Messages.UPDATE_JOB, job_update)
                
                ranges_update = self.communicator.fetch_ranges_update()
                if ranges_update:
                    self.logging_actor.info.remote(self.id, f"Fetched rangesupdate from communicator: {ranges_update}")
                    return self.return_message(Messages.UPDATE_EVENT_RANGES, ranges_update)

                time.sleep(1) # Nothing to do, sleeping...
        
        return self.return_message(Messages.IDLE)
