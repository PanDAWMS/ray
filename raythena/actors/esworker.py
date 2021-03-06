import json
import os
import re
import shutil
import time
from typing import Union, Tuple, List, Dict

import ray

from raythena.actors.loggingActor import LoggingActor
from raythena.utils.config import Config
from raythena.utils.eventservice import EventRangeRequest, Messages, EventRangeUpdate, PandaJob, EventRange
from raythena.utils.exception import IllegalWorkerState, StageInFailed, StageOutFailed
from raythena.utils.plugins import PluginsRegistry
from raythena.utils.ray import get_node_ip
from raythena.utils.timing import CPUMonitor
from raythena.actors.payloads.basePayload import BasePayload
from raythena.actors.payloads.eventservice.esPayload import ESPayload


@ray.remote(num_cpus=1)
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
    TRANSITIONS_EVENTSERVICE = {
        READY_FOR_JOB: [JOB_REQUESTED],
        JOB_REQUESTED: [STAGE_IN, DONE],
        STAGE_IN: [READY_FOR_EVENTS],
        READY_FOR_EVENTS: [EVENT_RANGES_REQUESTED],
        EVENT_RANGES_REQUESTED: [FINISHING_LOCAL_RANGES, PROCESSING],
        FINISHING_LOCAL_RANGES: [STAGE_OUT],
        PROCESSING: [READY_FOR_EVENTS],
        STAGE_OUT: [FINISHING],
        FINISHING: [DONE],
        DONE: [READY_FOR_JOB]
    }

    TRANSITIONS_STANDARD = {
        READY_FOR_JOB: [JOB_REQUESTED],
        JOB_REQUESTED: [STAGE_IN, DONE],
        STAGE_IN: [PROCESSING],
        PROCESSING: [STAGE_OUT],
        STAGE_OUT: [FINISHING],
        FINISHING: [DONE],
        DONE: [READY_FOR_JOB]
    }

    def __init__(self, actor_id: str, config: Config,
                 logging_actor: LoggingActor) -> None:
        """
        Initialize attributes, instantiate a payload and setup the workdir

        Args:
            actor_id: actor id
            config: application config
            logging_actor: remote logger
        """
        self.id = actor_id
        self.config = config
        self.logging_actor = logging_actor
        self.job = None
        self.transitions = ESWorker.TRANSITIONS_STANDARD
        self.node_ip = get_node_ip()
        self.state = ESWorker.READY_FOR_JOB
        self.payload_job_dir = None
        self.payload_actor_output_dir = None
        self.payload_actor_process_dir = None
        self.cpu_monitor = None
        self.first_event_range_request = True
        self.workdir = os.path.expandvars(
            self.config.ray.get('workdir', os.getcwd()))
        if not os.path.isdir(self.workdir):
            self.workdir = os.getcwd()
        self.plugin_registry = PluginsRegistry()
        payload = self.config.payload['plugin']
        self.payload_class = self.plugin_registry.get_plugin(payload)
        self.payload: Union[BasePayload, ESPayload] = self.payload_class(self.id, self.logging_actor,
                                                                         self.config)
        self.logging_actor.info.remote(self.id, "Ray worker started", time.asctime())

    def modify_job(self, job: Dict) -> Dict:
        """
        Modify the job dict before sending it to the payload.

        Returns:
            Dict
        """

        self.logging_actor.info.remote(self.id, "modify_job", time.asctime())
        if "jobPars" not in job:
            return job
        cmd = job["jobPars"]
        inputEVNTFile = re.findall(r"\-\-inputEVNTFile=([\w\.\,]*) \-", cmd)
        if len(inputEVNTFile) != 1:
            return job
        inFiles = [os.path.join(os.path.expandvars(self.config.harvester['endpoint']), x) for x in inputEVNTFile[0].split(",")]
        inFiles = ",".join(inFiles[0:1])
        self.logging_actor.info.remote(self.id, f"inFiles: {inFiles}", time.asctime())
        cmd = re.sub(r"\-\-inputEVNTFile=([\w\.\,]*) \-", f"--inputEVNTFile={inFiles} -", cmd)
        self.logging_actor.info.remote(self.id, f"cmd: {cmd}", time.asctime())
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
            self.logging_actor.warn.remote(
                self.id,
                f"Specified path {self.payload_job_dir} does not exist. Using cwd {os.getcwd()}",
                time.asctime()
            )
            self.payload_job_dir = self.workdir

        subdir = f"{self.id}"
        self.payload_actor_process_dir = os.path.join(self.payload_job_dir, subdir)
        self.payload_actor_output_dir = os.path.join(self.payload_job_dir, subdir, "esOutput")
        try:
            os.mkdir(self.payload_actor_process_dir)
            os.chdir(self.payload_actor_process_dir)
        except Exception:
            raise StageInFailed(self.id)
        try:
            self.logging_actor.debug.remote(self.id,
                                            f"Creating output dir {self.payload_actor_output_dir}",
                                            time.asctime())
            if not os.path.isdir(self.payload_actor_output_dir):
                os.mkdir(self.payload_actor_output_dir)
        except Exception:
            self.logging_actor.warn.remote(
                self.id,
                "Exception when creating the payload_actor_output_dir",
                time.asctime()
            )
        self.cpu_monitor = CPUMonitor(os.path.join(self.payload_actor_process_dir, "cpu_monitor.json"))
        self.cpu_monitor.start()

        self.payload.stagein()
        self.payload.start(self.modify_job(self.job))
        self.transition_state(ESWorker.READY_FOR_EVENTS if self.
                              is_event_service_job() else ESWorker.PROCESSING)

    def stageout(self) -> None:
        """
        Stage-out job output data

        Returns:
            None
        """
        self.logging_actor.info.remote(self.id, "Performing stageout", time.asctime())
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
            self.logging_actor.error.remote(
                self.id,
                f"Illegal transition from {ESWorker.STATES_NAME[self.state]} to {ESWorker.STATES_NAME[dest]}",
                time.asctime()
            )
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

    def set_transitions(self) -> None:
        """
        Set the allowed transitions depending on the type of job being processed

        Returns:
            None
        """
        if self.is_event_service_job():
            self.transitions = ESWorker.TRANSITIONS_EVENTSERVICE
        else:
            self.transitions = ESWorker.TRANSITIONS_STANDARD

    def receive_job(self, reply: int, job: PandaJob) -> Tuple[str, int, object]:
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
            self.set_transitions()
            self.logging_actor.debug.remote(
                self.id, "Received response to job request, starting stage-in", time.asctime())
            self.stagein()
            self.logging_actor.debug.remote(
                self.id, "finished job stage-in", time.asctime())
        else:
            self.transition_state(ESWorker.DONE)
            self.logging_actor.error.remote(
                self.id, "Could not fetch job. Set state to done.", time.asctime())

        return self.return_message(Messages.REPLY_OK)

    def mark_new_job(self) -> Tuple[str, int, object]:
        """
        Indicate that the worker should prepare to receive new jobs from the driver, should be called after the worker
        notifies that it reached the state 'DONE' if the actor should be re-used fo processing another job

        Returns:
            Job request message
        """
        self.transition_state(ESWorker.READY_FOR_JOB)
        return self.return_message(Messages.REQUEST_NEW_JOB)

    def receive_event_ranges(
            self, reply: int,
            event_ranges: List[EventRange]) -> Tuple[str, int, object]:
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
        self.logging_actor.debug.remote(
            self.id, f"Received response to event ranges request. ({len(event_ranges)} event ranges)", time.asctime())

        self.transition_state(ESWorker.PROCESSING)
        return self.return_message(Messages.REPLY_OK)

    def return_message(self,
                       message: int,
                       data: object = None) -> Tuple[str, int, object]:
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
        self.logging_actor.warn.remote(self.id,
                                       "Received interruption from driver", time.asctime())
        self.payload.stop()

    def terminate_actor(self) -> None:
        """
        End the processing of current job by stopping the payload and moving to state DONE

        Returns:
            None
        """
        self.logging_actor.info.remote(self.id, "stopping actor", time.asctime())
        self.payload.stop()
        self.cpu_monitor.stop()
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
            ranges_update: Dict[str, str]) -> Union[EventRangeUpdate, None]:
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
        self.logging_actor.info.remote(self.id, f"stageout_event_service_files: {ranges[self.job.get_id()]}", time.asctime())
        # stage-out finished event ranges
        for range_update in ranges[self.job.get_id()]:
            if "eventStatus" not in range_update:
                raise StageOutFailed(self.id)
            if range_update["eventStatus"] == "failed":
                self.logging_actor.info.remote(self.id, "event range failed, will not stage-out", time.asctime())
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
                range_update[cfile_key] = dst
                if os.path.isfile(cfile) and not os.path.isfile(dst):
                    shutil.move(cfile, dst)
        return ranges

    def get_message(self) -> Tuple[str, int, object]:
        """
        Used by the driver to retrieve messages from the worker. This function should be called regularly to make
        sure that the worker is able to process a job.

        Returns:
            Tuple depending on the current worker state indicating actions that should be performed by the driver
            to continue the processing
        """
        while self.state != ESWorker.DONE:
            if self.state == ESWorker.READY_FOR_JOB:
                # ready to get a new job
                self.transition_state(ESWorker.JOB_REQUESTED)
                self.logging_actor.debug.remote(
                    self.id, "Sending job request to the driver", time.asctime())
                return self.return_message(Messages.REQUEST_NEW_JOB)
            elif self.payload.is_complete():
                # payload process ended... Start stageout
                # if an exception occurs when changing state, this means that the payload ended early
                # send final job / event update
                self.logging_actor.info.remote(
                    self.id,
                    f"Payload ended with return code {self.payload.return_code()}",
                    time.asctime()
                )
                self.transition_state(ESWorker.STAGE_OUT)
                self.stageout()
                return self.return_message(Messages.PROCESS_DONE)
            elif self.is_event_service_job() and (
                    self.state == ESWorker.READY_FOR_EVENTS or
                    self.should_request_ranges()):
                req = EventRangeRequest()
                if not self.first_event_range_request:
                    # n_events = self.config.resources['corepernode'] * 2
                    n_events = self.config.resources['corepernode']
                else:
                    # First time request only for 'NCPU' events because
                    # Harvester gives 'NCPU * nodes' initially.
                    n_events = self.config.resources['corepernode']
                    self.logging_actor.debug.remote(self.id,
                                                    f"First event range request. Requesting {n_events} event ranges.", time.asctime())
                    self.first_event_range_request = False
                req.add_event_request(self.job['PandaID'],
                                      n_events,
                                      self.job['taskID'], self.job['jobsetID'])
                self.transition_state(ESWorker.EVENT_RANGES_REQUESTED)
                self.logging_actor.debug.remote(
                    self.id, "Sending event ranges request to the driver", time.asctime())
                return self.return_message(Messages.REQUEST_EVENT_RANGES, req)
            elif self.state == ESWorker.DONE:
                return self.return_message(Messages.PROCESS_DONE)
            else:
                job_update = self.payload.fetch_job_update()
                if job_update:
                    self.logging_actor.info.remote(
                        self.id,
                        f"Fetched jobupdate from payload: {job_update}", time.asctime())
                    return self.return_message(Messages.UPDATE_JOB, job_update)

                ranges_update = self.payload.fetch_ranges_update()
                if ranges_update:
                    self.logging_actor.debug.remote(self.id,
                                                    "Started stage-out of event service files to harvester workdir", time.asctime())
                    ranges_update = self.stageout_event_service_files(ranges_update)
                    self.logging_actor.debug.remote(self.id,
                                                    "Finished stage-out of event service files", time.asctime())
                    return self.return_message(Messages.UPDATE_EVENT_RANGES,
                                               ranges_update)

                time.sleep(1)  # Nothing to do, sleeping...

        return self.return_message(Messages.PROCESS_DONE)
