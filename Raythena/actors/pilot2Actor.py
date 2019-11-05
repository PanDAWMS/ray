import ray
import logging
import os
import asyncio
from asyncio.subprocess import PIPE, create_subprocess_shell
from Raythena.utils.ray import get_node_ip
from Raythena.utils.logging import configure_logger
from Raythena.utils.importUtils import import_from_string

logger = logging.getLogger(__name__)


@ray.remote
class Pilot2Actor:

    def __init__(self, actor_id, panda_queue, config):
        self.id = actor_id
        self.config = config
        self.panda_queue = panda_queue
        self.job = None
        self.eventranges = list()
        configure_logger(config)
        communicator = "pilot2_http:Actor"
        self.communicator_class = import_from_string(f"Raythena.actors.communicators.{communicator}")
        self.node_ip = get_node_ip()
        self.pilot_process = None
        self.communicator = self.communicator_class(self, self.config)
        self.communicator.start()
        logging.info(f"(Worker {self.node_ip}) : Ray worker started")

    def receive_job(self, job):
        self.job = job[0]
        logger.info(f"(Worker {self.node_ip}) : Received job {self.job}")
        command = self.build_pilot_command()
        command = self.communicator.fix_command(command)
        logger.info(f"Final payload command: {command}")
        self.pilot_process = self.asyncio_run_coroutine(create_subprocess_shell, command, stdout=PIPE, stderr=PIPE, executable='/bin/bash')
        logger.info(f"Started subprocess {self.pilot_process.pid}")
        return self.return_message('received_job')

    def receive_event_ranges(self, eventranges):
        self.eventranges.append(eventranges)
        logger.info(f"(Worker {self.node_ip}) : Received eventRanges {eventranges}")
        return self.return_message('received_event_range')

    def asyncio_run_coroutine(self, coroutine, *args, **kwargs):
        return asyncio.get_event_loop().run_until_complete(coroutine(*args, **kwargs))

    def build_pilot_command(self):
        """
        """
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
        logger.info(f"stopping actor {self.id}")
        pexit = self.pilot_process.returncode
        logger.debug(f"Pilot2 return code: {pexit}")
        if pexit is None:
            self.pilot_process.terminate()

        stdout, stderr = self.asyncio_run_coroutine(self.pilot_process.communicate)
        logger.info("========== Pilot stdout ==========")
        logger.info("\n" + str(stdout, encoding='utf-8'))
        logger.error("========== Pilot stderr ==========")
        logger.error("\n" + str(stderr, encoding='utf-8'))
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
        logger.info('get_message looping')

        return self.return_message(-1)
