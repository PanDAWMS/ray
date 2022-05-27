import json
import os
import re
import shutil
import time
from typing import Union, Tuple, Sequence, Any, Mapping, Optional

import datetime
import threading

from socket import gethostname
from time import sleep

import ray

from raythena.utils.logging import disable_stdout_logging, make_logger, log_to_file
from raythena.utils.config import Config
from raythena.utils.eventservice import EventRangeRequest, Messages, EventRangeUpdate, PandaJob, EventRange
from raythena.utils.exception import IllegalWorkerState, StageInFailed, StageOutFailed, WrappedException, BaseRaythenaException
from raythena.utils.ray import get_node_ip
# from raythena.utils.timing import CPUMonitor
from raythena.actors.payloads.basePayload import BasePayload
from raythena.actors.payloads.eventservice.esPayload import ESPayload

from raythena.actors.payloads.eventservice.pilothttp import PilotHttpPayload


# Type returned by the worker methods to the driver
WorkerResponse = Tuple[str, int, Any]


@ray.remote(num_cpus=1, max_restarts=1, max_task_retries=3)
class ESWorker(object):
    """
    Actor running on HPC compute node. Each actor will start a payload plugin which handle the job processing as well
    as the communication with the job processing framework, Athena or any intermediary layer such as pilot 2.

    A worker instance is a stateful object which basically transitions from
    job request -> stage-in -> processing <-> ranges request -> stage-out -> done
    Allowed transition are defined by ESWorker.TRANSITIONS_EVENTSERVICE (for event service job)
    and ESWorker.TRANSITIONS_STANDARD (for standard job)

    The current state defines what message will be sent to the driver when
    it requests the worker state using get_message(). The driver needs to frequently call get_message() and process
    requests from the worker, allowing the worker to progress in the job processing.
    """

    READY_FOR_JOB = 0  # initial state, before the first job request
    JOB_REQUESTED = 1  # job has been requested to the driver, waiting for result
    READY_FOR_EVENTS = 2  # ready to request new events for the current job
    EVENT_RANGES_REQUESTED = 3  # event ranges have been requested to the driver, waiting for result
    FINISHING_LOCAL_RANGES = 4  # do not request additional ranges, will move to STAGE_OUT once local cache is empty
    PROCESSING = 5  # currently processing event ranges
    FINISHING = 6  # Performing cleanup of resources, preparing final server update
    DONE = 7  # Actor has finished processing job
    STAGE_IN = 8  # Staging-in data.
    STAGE_OUT = 9  # Staging-out data

    STATES_NAME = {
        READY_FOR_JOB: "READY_FOR_JOB",
        JOB_REQUESTED: "JOB_REQUESTED",
        READY_FOR_EVENTS: "READY_FOR_EVENTS",
        EVENT_RANGES_REQUESTED: "EVENT_RANGES_REQUESTED",
        FINISHING_LOCAL_RANGES: "FINISHING_LOCAL_RANGES",
        PROCESSING: "PROCESSING",
        FINISHING: "FINISHING",
        DONE: "DONE",
        STAGE_IN: "STAGE_IN",
        STAGE_OUT: "STAGE_OUT"
    }

    # authorize state transition from x to y if y in TRANSITION[X]
    TRANSITIONS = {
        READY_FOR_JOB: [JOB_REQUESTED],
        JOB_REQUESTED: [STAGE_IN, DONE],
        STAGE_IN: [READY_FOR_EVENTS],
        READY_FOR_EVENTS: [EVENT_RANGES_REQUESTED, STAGE_OUT],
        EVENT_RANGES_REQUESTED: [FINISHING_LOCAL_RANGES, PROCESSING, STAGE_OUT],
        FINISHING_LOCAL_RANGES: [STAGE_OUT],
        PROCESSING: [READY_FOR_EVENTS, STAGE_OUT],
        STAGE_OUT: [FINISHING],
        FINISHING: [DONE],
        DONE: [READY_FOR_JOB]
    }

    def __init__(self, actor_id: str, config: Config,
                 session_log_dir: str, job: PandaJob = None, event_ranges: Sequence[EventRange] = None) -> None:
        """
        Initialize attributes, instantiate a payload and setup the workdir

        Args:
            actor_id: actor id
            config: application config
            logging_actor: remote logger
        """
        self.id = actor_id
        self.config = config
        self._logger = make_logger(self.config, self.id)
        self.session_log_dir = session_log_dir
        self.job = None
        self.transitions = ESWorker.TRANSITIONS
        self.node_ip = get_node_ip()
        self.state = ESWorker.READY_FOR_JOB
        self.payload_job_dir = None
        self.payload_actor_output_dir = None
        self.payload_actor_process_dir = None
        self.actor_ray_logs_dir = None
        self.cpu_monitor = None
        self.workdir = os.path.expandvars(
            self.config.ray.get('workdir', os.getcwd()))
        if not os.path.isdir(self.workdir):
            self.workdir = os.getcwd()
        self.pilot_kill_file = os.path.expandvars(self.config.payload.get('pilotkillfile', 'pilot_kill_payload'))
        self.pilot_kill_time = self.config.payload.get('pilotkilltime', 600)
        self.time_monitor_file = os.path.expandvars(self.config.payload.get('timemonitorfile', 'RaythenaTimeMonitor.txt'))
        self.payload: Union[BasePayload, ESPayload] = PilotHttpPayload(self.id, self.config)
        self.start_time = -1
        self.time_limit = -1
        self.elapsed = 1
        if job:
            self.transition_state(ESWorker.JOB_REQUESTED)
            self.receive_job(Messages.REPLY_OK, job)
            if event_ranges:
                self.transition_state(ESWorker.EVENT_RANGES_REQUESTED)
                self.receive_event_ranges(Messages.REPLY_OK, event_ranges)

    def check_time(self) -> None:
        while True:
            curtime = datetime.datetime.now()
            time_elapsed = curtime.hour * 3600 + curtime.minute * 60 + curtime.second - self.start_time
            if time_elapsed <= 0:
                time_elapsed = 24 * 3600 + time_elapsed
            if time_elapsed // 300 >= self.elapsed:
                self.elapsed += 1
                try:
                    if self.config.logging.get('copyraylogs', False):
                        if os.path.isdir(self.actor_ray_logs_dir):
                            shutil.rmtree(self.actor_ray_logs_dir)
                        shutil.copytree(self.session_log_dir, self.actor_ray_logs_dir)
                except Exception as e:
                    self._logger.warning(f"Failed to copy ray logs to actor directory: {e}")
            if time_elapsed > self.time_limit - self.pilot_kill_time:
                killsignal = open(self.pilot_kill_file, 'w')
                killsignal.close()
                self._logger.info("killsignal sent to payload")
                break
            else:
                sleep(5)

    def modify_job(self, job: PandaJob) -> PandaJob:
        """
        Modify the job dict before sending it to the payload.

        Returns:
            Dict
        """
        if "jobPars" not in job:
            return job
        cmd = job["jobPars"]
        inputEVNTFile = re.findall(r"\-\-inputEVNTFile=([\w\.\,]*) \-", cmd)
        if len(inputEVNTFile) != 1:
            return job
        inFiles = [os.path.join(os.path.expandvars(self.config.harvester['endpoint']), x) for x in inputEVNTFile[0].split(",")]
        inFiles = ",".join(inFiles[0:1])
        cmd = re.sub(r"\-\-inputEVNTFile=([\w\.\,]*) \-", f"--inputEVNTFile={inFiles} -", cmd)
        job["jobPars"] = cmd
        return job

    def stagein(self) -> None:
        """
        Perform a generic stage-in, creating a unique worker directory and cwd to it,
        moving input files to that directory using a symlink then starts the payload

        Returns:
            None
        """
        self.payload_job_dir = os.path.join(self.workdir, self.job['PandaID'])
        if not os.path.isdir(self.payload_job_dir):
            self._logger.warning(f"Specified path {self.payload_job_dir} does not exist. Using cwd {os.getcwd()}")
            self.payload_job_dir = self.workdir

        subdir = f"{self.id}"
        self.payload_actor_process_dir = os.path.join(self.payload_job_dir, subdir)
        self.payload_actor_output_dir = os.path.join(self.payload_job_dir, subdir, "esOutput")
        self.actor_ray_logs_dir = os.path.join(self.payload_actor_process_dir, "ray_logs")
        try:
            time_limit_monitor = open(os.path.join(self.workdir, self.time_monitor_file))
            start_time = time_limit_monitor.readline().split(':')
            self.start_time = int(start_time[0]) * 3600 + int(start_time[1]) * 60 + int(start_time[2])
            time_limit = time_limit_monitor.readline().split(':')
            if len(time_limit) < 3:
                time_limit = [0] + time_limit
            self.time_limit = int(time_limit[0]) * 3600 + int(time_limit[1]) * 60 + int(time_limit[2])
            timer_thread = threading.Thread(name='timer', target=self.check_time, daemon=True)
            timer_thread.start()
        except Exception as e:
            self._logger.warning(f"Failed to setup timer thread: {e}")

        try:
            os.mkdir(self.payload_actor_process_dir)
            os.chdir(self.payload_actor_process_dir)
            worker_logfile = self.config.logging.get('workerlogfile', None)
            if worker_logfile:
                log_to_file(self.config.logging.get('level', 'warning').upper(), os.path.join(self.payload_actor_process_dir, os.path.basename(worker_logfile)))
                disable_stdout_logging()

            self._logger.info(f"Ray worker started on node {gethostname()}")

            if not os.path.isdir(self.payload_actor_output_dir):
                os.mkdir(self.payload_actor_output_dir)
        except Exception as e:
            self._logger.warning(f"Exception when creating dir: {e}")
            raise StageInFailed(self.id)
        # self.cpu_monitor = CPUMonitor(os.path.join(self.payload_actor_process_dir, "cpu_monitor.json"))
        # self.cpu_monitor.start()
        try:
            self.payload.stagein()
            self.payload.start(self.modify_job(self.job))
        except Exception as e:
            self._logger.warning(f"Failed to stagein payload: {e}")
            raise StageInFailed(self.id)
        self.transition_state(ESWorker.READY_FOR_EVENTS if self.
                              is_event_service_job() else ESWorker.PROCESSING)

    def stageout(self) -> None:
        """
        Stage-out job output data

        Returns:
            None
        """
        # TODO move payload out file to harvester dir, drain jobupdate and rangeupdate from payload
        self.payload.stageout()
        self.transition_state(ESWorker.FINISHING)
        self.terminate_actor()

    def transition_state(self, dest: int) -> None:
        """
        Performs transition to the destination state.

        Args:
            dest: state to transit to

        Returns:
            None

        Raises:
            IllegalWorkerState if the transition isn't allowed
        """
        if dest not in self.transitions[self.state]:
            self._logger.error(f"Illegal transition from {ESWorker.STATES_NAME[self.state]} to {ESWorker.STATES_NAME[dest]}")
            raise IllegalWorkerState(worker_id=self.id,
                                     src_state=ESWorker.STATES_NAME[self.state],
                                     dst_state=ESWorker.STATES_NAME[dest])
        self.state = dest

    def is_event_service_job(self) -> bool:
        """
        Checks if the current job is an event service job

        Returns:
            True if current job is an eventservice job
        """
        return self.job and self.job['eventService']

    def receive_job(self, reply: int, job: PandaJob) -> WorkerResponse:
        """
        Assign a job to the worker. If a job was successfully assigned, move to the stage-in otherwise end the actor

        Args:
            reply: status code indicating whether the job request was correctly processed
            job: panda job specification

        Returns:
            tuple with status code indicating that the job was correctly received
        """
        self.job = job
        if reply == Messages.REPLY_OK and self.job:
            self.transition_state(ESWorker.STAGE_IN)
            try:
                self.stagein()
            except BaseRaythenaException:
                raise
            except Exception as e:
                raise WrappedException(self.id, e)
        else:
            self.transition_state(ESWorker.DONE)
            self._logger.error("Could not fetch job. Set state to done.")

        return self.return_message(Messages.REPLY_OK)

    def mark_new_job(self) -> WorkerResponse:
        """
        Indicate that the worker should prepare to receive new jobs from the driver, should be called after the worker
        notifies that it reached the state 'DONE' if the actor should be re-used fo processing another job

        Returns:
            Job request message
        """
        self.transition_state(ESWorker.READY_FOR_JOB)
        self.transition_state(ESWorker.JOB_REQUESTED)
        return self.return_message(Messages.REQUEST_NEW_JOB)

    def receive_event_ranges(
            self, reply: int,
            event_ranges: Sequence[EventRange]) -> WorkerResponse:
        """
        Sends event ranges to the worker. Update the PFN of event ranges to an absolute path if
        it is an relative path

        Args:
            reply: status code indicating whether the event ranges request was correctly processed
            event_ranges: list of event ranges to process

        Returns:
            tuple with status code indicating that the event ranges were correctly received
        """
        if reply == Messages.REPLY_NO_MORE_EVENT_RANGES or not event_ranges:
            # no new ranges... finish processing local cache then terminate actor
            self.transition_state(ESWorker.FINISHING_LOCAL_RANGES)
            self.payload.submit_new_ranges(None)
            return self.return_message(Messages.REPLY_OK)
        for crange in event_ranges:
            if not os.path.isabs(crange.PFN):
                crange.PFN = os.path.join(
                    os.path.expandvars(self.config.harvester['endpoint']),
                    crange.PFN)
        self.payload.submit_new_ranges(event_ranges)

        self.transition_state(ESWorker.PROCESSING)
        return self.return_message(Messages.REPLY_OK)

    def return_message(self,
                       message: int,
                       data: Any = None) -> WorkerResponse:
        """
        Utility function building a tuple returned to the driver

        Args:
            message: message type to send to the driver
            data: extra data attached to the message type

        Returns:
            Tuple of (id, message, data)
        """
        return self.id, message, data

    def interrupt(self) -> None:
        """
        Notifies the worker that it should stop processing the current job, stop the payload which will then
        trigger the stage-out of the worker

        Returns:
            None
        """
        self.payload.stop()

    def terminate_actor(self) -> None:
        """
        End the processing of current job by stopping the payload and moving to state DONE

        Returns:
            None
        """
        self.payload.stop()
        # self.cpu_monitor.stop()
        self.transition_state(ESWorker.DONE)

    def should_request_ranges(self) -> bool:
        """
        Checks if the worker should ask more event ranges to the driver.

        Returns:
            True if more event ranges are are needed
        """
        # do not transition if not in a state allowing for event ranges request
        if ESWorker.READY_FOR_EVENTS not in self.transitions[self.state]:
            return False

        res = self.payload.should_request_more_ranges()
        if res:
            self.transition_state(ESWorker.READY_FOR_EVENTS)
        return res

    def get_no_more_ranges(self) -> bool:
        """
        Returns a boolean indicating whether any events remain to be processed

        Returns:
            True if no more ranges to be processed
        """
        return self.payload.get_no_more_ranges()

    def stageout_event_service_files(
            self,
            ranges_update: Mapping[str, str]) -> Optional[EventRangeUpdate]:
        """
        Move the event ranges files reported in the event ranges update to the harvester endpoint common to
        all workers for stage-out

        Args:
            ranges_update: event ranges updated received by the payload

        Returns:
            event ranges update referencing moved output files
        """
        harvester_endpoint = os.path.expandvars(self.config.harvester.get("endpoint", ""))
        if not os.path.isdir(harvester_endpoint):
            return
        ranges = json.loads(ranges_update['eventRanges'][0])
        ranges = EventRangeUpdate.build_from_dict(self.job.get_id(), ranges)
        # stage-out finished event ranges
        for range_update in ranges[self.job.get_id()]:
            if "eventStatus" not in range_update:
                raise StageOutFailed(self.id)
            if range_update["eventStatus"] == "failed":
                self._logger.warning("event range failed, will not stage-out")
                continue
            if "path" in range_update and range_update["path"]:
                cfile_key = "path"
            else:
                raise StageOutFailed(self.id)
            cfile = range_update.get(cfile_key, None)
            if cfile:
                dst = os.path.join(
                    self.payload_actor_output_dir,
                    os.path.basename(cfile) if os.path.isabs(cfile) else cfile)
                if os.path.isfile(cfile) and not os.path.isfile(dst):
                    shutil.move(cfile, dst)
                    range_update[cfile_key] = dst
        return ranges

    def get_payload_message(self) -> Optional[WorkerResponse]:
        """
        Returns:
            A payload message (event ranges or job update) to be sent to the driver.
        """
        ranges_update = self.payload.fetch_ranges_update()
        if ranges_update:
            ranges_update = self.stageout_event_service_files(ranges_update)
            return self.return_message(Messages.UPDATE_EVENT_RANGES,
                                       ranges_update)

        job_update = self.payload.fetch_job_update()
        if job_update:
            return self.return_message(Messages.UPDATE_JOB, job_update)
        return None

    def get_message(self) -> WorkerResponse:
        """
        Used by the driver to retrieve messages from the worker. This function should be called regularly to make
        sure that the worker is able to process a job.

        Returns:
            Tuple depending on the current worker state indicating actions that should be performed by the driver
            to continue the processing
        """
        try:
            while self.state != ESWorker.DONE:
                payload_message = self.get_payload_message()
                if payload_message:
                    return payload_message
                elif self.state == ESWorker.READY_FOR_JOB:
                    # ready to get a new job
                    self.transition_state(ESWorker.JOB_REQUESTED)
                    return self.return_message(Messages.REQUEST_NEW_JOB)
                elif self.payload.is_complete():
                    # check if there are any remaining message from the payload in queue.
                    payload_message = self.get_payload_message()
                    if payload_message:
                        # if so, return one message
                        return payload_message
                    else:
                        # if no more message, proceed to stage-out
                        self.transition_state(ESWorker.STAGE_OUT)
                        self.stageout()
                        return self.return_message(Messages.PROCESS_DONE)
                elif self.is_event_service_job() and (
                        self.state == ESWorker.READY_FOR_EVENTS or
                        self.should_request_ranges()):
                    req = EventRangeRequest()
                    req.add_event_request(self.job['PandaID'],
                                          self.config.resources.get('corepernode', 64),
                                          self.job['taskID'], self.job['jobsetID'])
                    self.transition_state(ESWorker.EVENT_RANGES_REQUESTED)
                    return self.return_message(Messages.REQUEST_EVENT_RANGES, req)
                elif self.state == ESWorker.DONE:
                    return self.return_message(Messages.PROCESS_DONE)
                else:
                    time.sleep(1)  # Nothing to do, sleeping...

            return self.return_message(Messages.PROCESS_DONE)
        except BaseRaythenaException:
            raise
        except Exception as e:
            raise WrappedException(self.id, e)
