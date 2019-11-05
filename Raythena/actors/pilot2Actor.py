import ray
import logging
import os
import asyncio
import signal
from Raythena.utils.ray import get_node_ip
from Raythena.utils.logging import configure_logger
from Raythena.utils.importUtils import import_from_string
from subprocess import Popen, PIPE

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
        self.pilot_process = Popen(command, shell=True, stdout=PIPE, stderr=PIPE, executable="/bin/bash")
        return self.return_message('received_job')

    def receive_event_ranges(self, eventranges):
        self.eventranges.append(eventranges)
        logger.info(f"(Worker {self.node_ip}) : Received eventRanges {eventranges}")
        return self.return_message('received_event_range')

    def build_pilot_command(self):
        """
        """
        conda_activate = os.path.join(self.config.conda_bin, 'activate')
        cmd = str()
        if os.path.isfile(conda_activate) and self.config.pilot_venv is not None:
            cmd += f"source {conda_activate} {self.config.pilot_venv};"
        prodSourceLabel = self.job['prodSourceLabel']
        pilot_bin = os.path.join(self.config.pilot_dir, "pilot.py")
        cmd += f"python {pilot_bin} -q {self.panda_queue} -r {self.panda_queue} -s {self.panda_queue} " \
               f"-i PR -j {prodSourceLabel} --pilot-user=ATLAS -t -w generic --url=http://127.0.0.1 " \
               f"-p 8080 -d --allow-same-user=False --resource-type MCORE;"
        return cmd

    def return_message(self, message):
        return self.id, message

    def terminate_actor(self):
        logger.info(f"stopping actor {self.id}")
        self.pilot_process.send_signal(signal.SIGTERM)

        self.communicator.stop()
        # stop the communicator first since communicate blocks the thread. communicate() should be moved to another thread,
        # wait for its completion using while not join() -> async_sleep()
        stdout, stderr = self.pilot_process.communicate()
        logger.info("========== Pilot stdout ==========")
        logger.info("\n" + str(stdout, encoding='utf-8'))
        logger.error("========== Pilot stderr ==========")
        logger.error("\n" + str(stderr, encoding='utf-8'))

    def async_sleep(self, delay):
        asyncio.get_event_loop().run_until_complete(asyncio.sleep(delay))

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
