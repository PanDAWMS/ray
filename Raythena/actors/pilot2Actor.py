import ray
import os
import asyncio
from asyncio.subprocess import PIPE, create_subprocess_shell
from Raythena.utils.ray import get_node_ip
from Raythena.utils.importUtils import import_from_string


@ray.remote
class Pilot2Actor:

    def __init__(self, actor_id, panda_queue, config, logging_actor):
        self.id = actor_id
        self.config = config
        self.panda_queue = panda_queue
        self.logging_actor = logging_actor
        self.job = None
        self.eventranges = list()
        communicator = "pilot2_http:Actor"
        self.communicator_class = import_from_string(f"Raythena.actors.communicators.{communicator}")
        self.node_ip = get_node_ip()
        self.pilot_process = None
        self.communicator = self.communicator_class(self, self.config)
        self.communicator.start()
        self.logging_actor.info.remote(self.id, "Ray worker started")

    def receive_job(self, job):
        self.job = job[0]
        self.logging_actor.debug.remote(self.id, f"Received job {self.job}")
        command = self.build_pilot_command()
        command = self.communicator.fix_command(command)
        self.logging_actor.info.remote(self.id, f"Final payload command: {command}")
        self.pilot_process = self.asyncio_run_coroutine(create_subprocess_shell, command, stdout=PIPE, stderr=PIPE, executable='/bin/bash')
        self.logging_actor.info.remote(self.id, f"Started subprocess {self.pilot_process.pid}")
        return self.return_message('received_job')

    def receive_event_ranges(self, eventranges):
        self.eventranges.append(eventranges)
        self.logging_actor.debug.remote(self.id, f"Received eventRanges {eventranges}")
        return self.return_message('received_event_range')

    def asyncio_run_coroutine(self, coroutine, *args, **kwargs):
        return asyncio.get_event_loop().run_until_complete(coroutine(*args, **kwargs))

    def build_pilot_command(self):
        """
        """
        cwd = os.getcwd()
        pilot_dir = os.path.expandvars(self.config.pilot.get('workdir', cwd))
        if not os.path.isdir(pilot_dir):
            self.logging_actor.warn.remote(self.id, f"Specified path {pilot_dir} does not exist. Using cwd {cwd}")
            pilot_dir = cwd

        subdir = f"{self.id}_{os.getpid()}"
        pilot_process_dir = os.path.join(pilot_dir, subdir)
        os.mkdir(pilot_process_dir)
        input_files = self.job['inFiles'].split(",")
        for input_file in input_files:
            in_abs = input_file if os.path.isabs(input_file) else os.path.join(pilot_dir, input_file)
            if os.path.isfile(in_abs):
                basename = os.path.basename(in_abs)
                os.symlink(in_abs, os.path.join(pilot_process_dir, basename))

        os.chdir(pilot_process_dir)

        conda_activate = os.path.join(self.config.conda_bin, 'activate')
        cmd = str()
        if os.path.isfile(conda_activate) and self.config.pilot_venv is not None:
            cmd += f"source {conda_activate} {self.config.pilot_venv};"
        prodSourceLabel = self.job['prodSourceLabel']

        pilot_bin = os.path.join(self.config.pilot_dir, "pilot.py")
        # use exec to replace the shell process with python. Allows to send signal to the python process if needed
        cmd += f"exec python {pilot_bin} -q {self.panda_queue} -r {self.panda_queue} -s {self.panda_queue} " \
               f"-i PR -j {prodSourceLabel} --pilot-user=ATLAS -t -w generic --url=http://127.0.0.1 " \
               f"-p 8080 -d --allow-same-user=False --resource-type MCORE;"
        return cmd

    def return_message(self, message):
        return self.id, message

    def terminate_actor(self):
        self.logging_actor.info.remote(self.id, f"stopping actor")
        pexit = self.pilot_process.returncode
        self.logging_actor.debug.remote(self.id, f"Pilot2 return code: {pexit}")
        if pexit is None:
            self.pilot_process.terminate()

        stdout, stderr = self.asyncio_run_coroutine(self.pilot_process.communicate)
        #self.logging_actor.info.remote(self.id, "========== Pilot stdout ==========")
        #self.logging_actor.info.remote(self.id, "\n" + str(stdout, encoding='utf-8'))
        #self.logging_actor.error.remote(self.id, "========== Pilot stderr ==========")
        #self.logging_actor.error.remote(self.id, "\n" + str(stderr, encoding='utf-8'))
        self.communicator.stop()

    def async_sleep(self, delay):
        self.asyncio_run_coroutine(asyncio.sleep, delay)

    def get_message(self):
        """
        Return a message to the driver depending on actor state
        """
        #while True:
        if not self.job:
            return self.return_message(0)
        if not self.eventranges:
            return self.return_message(1)
        self.async_sleep(1)
        # self.logging_actor.info.remote(self.id, 'get_message looping')
        if self.pilot_process is not None and self.pilot_process.returncode is not None:
            return self.return_message(2)
        return self.return_message(-1)
