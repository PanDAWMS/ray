from asyncio import subprocess
import os
import shutil
import tempfile
import time
import traceback
from math import ceil
from queue import Empty, Queue
from socket import gethostname
from typing import (Any, Dict, Iterator, List, Mapping, Sequence, Iterable,
                    Tuple)
from subprocess import DEVNULL, Popen

import ray
from ray.exceptions import RayActorError
from ray.types import ObjectRef
from raythena import __version__
from raythena.actors.esworker import ESWorker, WorkerResponse
from raythena.drivers.baseDriver import BaseDriver
from raythena.drivers.communicators.baseCommunicator import (BaseCommunicator,
                                                             RequestData)
from raythena.drivers.communicators.harvesterFileMessenger import \
    HarvesterFileCommunicator
from raythena.utils.bookkeeper import BookKeeper, TaskStatus
from raythena.utils.config import Config
from raythena.utils.eventservice import (EventRange, EventRangeDef,
                                         EventRangeRequest, EventRangeUpdate,
                                         JobDef, Messages,
                                         PandaJobRequest
                                         )
from raythena.utils.exception import BaseRaythenaException
from raythena.utils.logging import (disable_stdout_logging, log_to_file,
                                    make_logger)
from raythena.utils.ray import build_nodes_resource_list


class ESDriver(BaseDriver):
    """
    The driver is managing all the ray workers and handling the communication with Harvester. It keeps tracks of
    which event ranges is assigned to which actor using a BookKeeper instance which provides the interface to read and update the status of each event range.

    It will also send requests for jobs, event ranges or update of produced output to harvester by using a communicator instance.
    The communicator uses the shared file system to communicate with Harvester and does I/O in a separate thread,
    communication between the driver and the communicator is done by message passing using a queue.

    The driver is starting one actor per node in the ray cluster except for the ray head node which doesn't execute
    any worker

    After creating all the actors, the driver will call the function get_message() of each actor.
    Futures are awaited, depending on the message returned by the actors, the driver will
    process it and call the appropriate remote function of the actor
    """

    def __init__(self, config: Config, session_dir: str) -> None:
        """
        Initialize logging, a bookKeeper instance, communicator and other attributes.
        An initial job request is also sent to Harvester.

        Args:
            config: application config
        """
        super().__init__(config, session_dir)
        self.id = "Driver"
        self._logger = make_logger(self.config, self.id)
        self.session_log_dir = os.path.join(self.session_dir, "logs")
        self.nodes = build_nodes_resource_list(self.config, run_actor_on_head=False)

        self.requests_queue: Queue[RequestData] = Queue()
        self.jobs_queue: Queue[Mapping[str, JobDef]] = Queue()
        self.event_ranges_queue: Queue[Mapping[str, Sequence[EventRangeDef]]] = Queue()

        workdir = os.path.expandvars(self.config.ray.get('workdir'))
        if not workdir or not os.path.exists(workdir):
            workdir = os.getcwd()
        self.config.ray['workdir'] = workdir
        self.workdir = workdir
        logfile = self.config.logging.get("driverlogfile", None)
        if logfile:
            log_to_file(self.config.logging.get("level", None), logfile)
            # TODO removing stdout on the root logger will also disable ray logging and collected stdout from actors
            disable_stdout_logging()

        self._logger.debug(f"Raythena v{__version__} initializing, running Ray {ray.__version__} on {gethostname()}")

        task_workdir_path_file = f"{workdir}/task_workdir_path.txt"
        if not os.path.isfile(task_workdir_path_file):
            self._logger.error(f"File {task_workdir_path_file} doesn't exist")
            return

        with open(task_workdir_path_file, 'r') as f:
            self.output_dir = f.readline()
        self.config.ray["outputdir"] = self.output_dir
        self.tar_merge_es_output_dir = self.output_dir
        self.tar_merge_es_files_dir = self.output_dir
        # self.cpu_monitor = CPUMonitor(os.path.join(workdir, "cpu_monitor_driver.json"))
        # self.cpu_monitor.start()

        self.communicator: BaseCommunicator = HarvesterFileCommunicator(self.requests_queue,
                                                                        self.jobs_queue,
                                                                        self.event_ranges_queue,
                                                                        self.config)
        self.communicator.start()
        self.requests_queue.put(PandaJobRequest())
        self.actors: Dict[str, ESWorker] = dict()
        self.pending_objectref_to_actor: Dict[ObjectRef, str] = dict()
        self.actors_message_queue = list()
        self.bookKeeper = BookKeeper(self.config)
        self.terminated = list()
        self.running = True
        self.n_eventsrequest = 0
        self.max_retries_error_failed_tasks = 3
        self.first_event_range_request = True
        self.no_more_events = False
        self.cache_size_factor = self.config.ray.get('cachesizefactor', 3)
        self.cores_per_node = self.config.resources.get('corepernode', os.cpu_count())
        self.n_actors = len(self.nodes)
        self.events_cache_size = self.cores_per_node * self.n_actors * self.cache_size_factor
        self.timeoutinterval = self.config.ray['timeoutinterval']
        self.max_running_merge_transforms = self.config.ray['mergemaxprocesses']
        self.panda_taskid = None
        # {input_filename, {merged_output_filename, ([(event_range_id, EventRange)], subprocess handle)}}
        self.running_merge_transforms: Dict[str, Dict[str, Tuple[List[Tuple[str, EventRange]], Popen]]] = dict()
        self.total_running_merge_transforms = 0
        self.failed_actor_tasks_count = dict()
        self.available_events_per_actor = 0
        self.total_tar_tasks = 0
        self.remote_jobdef_byid = dict()
        self.config_remote = ray.put(self.config)

        # create the output directories if needed
        try:
            if not os.path.isdir(self.output_dir):
                os.mkdir(self.output_dir)
        except Exception:
            self._logger.warning(f"Exception when creating the {self.output_dir}")
            raise
        try:
            if not os.path.isdir(self.tar_merge_es_output_dir):
                os.mkdir(self.tar_merge_es_output_dir)
        except Exception:
            self._logger.warning(f"Exception when creating the {self.tar_merge_es_output_dir}")
            raise
        try:
            if not os.path.isdir(self.tar_merge_es_files_dir):
                os.mkdir(self.tar_merge_es_files_dir)
        except Exception:
            self._logger.warning(f"Exception when creating the {self.tar_merge_es_files_dir}")
            raise

    def __str__(self) -> str:
        """
        String representation of driver attributes

        Returns:
            self.__dict__ string repr
        """
        return self.__dict__.__str__()

    def __getitem__(self, key: str) -> ESWorker:
        """
        Retrieve actor by worker_id

        Args:
            key: actor worker_id

        Returns:
            The actor
        """
        return self.actors[key]

    def start_actors(self) -> None:
        """
        Initialize actor communication by performing the first call to get_message() and add the future to the future list.

        Returns:
            None
        """
        for actor_id, worker in self.actors.items():
            self.enqueue_actor_call(actor_id, worker.get_message.remote())

    def create_actors(self) -> None:
        """
        Create actors on each node. Before creating an actor, the driver tries to assign it a job and an initial batch of
        event ranges. This avoid having all actors requesting jobs and event ranges at the start of the job.

        Returns:
            None
        """
        events_per_actor = min(self.available_events_per_actor, self.cores_per_node)
        for i, node in enumerate(self.nodes):
            nodeip = node['NodeManagerAddress']
            node_constraint = f"node:{nodeip}"
            actor_id = f"Actor_{i}"
            kwargs = {
                'actor_id': actor_id,
                'config': self.config_remote,
                'session_log_dir': self.session_log_dir
            }
            job = self.bookKeeper.assign_job_to_actor(actor_id)
            if job:
                job_remote = self.remote_jobdef_byid[job['PandaID']]
                kwargs['job'] = job_remote
                event_ranges = self.bookKeeper.fetch_event_ranges(actor_id, events_per_actor)
                if event_ranges:
                    kwargs['event_ranges'] = event_ranges
                    self._logger.debug(
                        f"Prefetched job {job['PandaID']} and {len(event_ranges)} event ranges for {actor_id}")

            actor = ESWorker.options(resources={node_constraint: 1}).remote(**kwargs)
            self.actors[actor_id] = actor

    def retrieve_actors_messages(self, ready: Sequence[ObjectRef]) -> Iterator[WorkerResponse]:
        """
        Given a list of ready futures from actors, unwrap them and return an interable over the result of each future.
        In case one of the futures raised an exception, the exception is handled by this function and not propagated to the caller.

        Args:
            ready: a list of read futures

        Returns:
            Iterator of WorkerResponse
        """
        try:
            messages = ray.get(ready)
        except Exception:
            # if any of the future raised an exception, we need to handle them one by one to know which one produced the exception.
            for r in ready:
                try:
                    actor_id, message, data = ray.get(r)
                except BaseRaythenaException as e:
                    self.handle_actor_exception(e.worker_id, e)
                except RayActorError as e:
                    self._logger.error(f"RayActorError: {e.error_msg}")
                except Exception as e:
                    self._logger.error(f"Caught exception while fetching result from {self.pending_objectref_to_actor[r]}: {e}")
                else:
                    yield actor_id, message, data
        else:
            self._logger.debug(f"Start handling messages batch of {len(messages)} actors")
            for actor_id, message, data in messages:
                yield actor_id, message, data

    def enqueue_actor_call(self, actor_id: str, future: ObjectRef):
        self.actors_message_queue.append(future)
        self.pending_objectref_to_actor[future] = actor_id

    def handle_actors(self) -> None:
        """
        Main function handling messages from all ray actors and dispatching to the appropriate handling function according to the message returned by the actor,

        Returns:
            None
        """
        new_messages, self.actors_message_queue = self.wait_on_messages()
        total_sent = 0
        while new_messages and self.running:
            for actor_id, message, data in self.retrieve_actors_messages(new_messages):
                if message == Messages.IDLE or message == Messages.REPLY_OK:
                    self.enqueue_actor_call(actor_id, self[actor_id].get_message.remote())
                elif message == Messages.REQUEST_NEW_JOB:
                    self.handle_job_request(actor_id)
                elif message == Messages.REQUEST_EVENT_RANGES:
                    total_sent = self.handle_request_event_ranges(actor_id, data, total_sent)
                elif message == Messages.UPDATE_JOB:
                    self.handle_update_job(actor_id, data)
                elif message == Messages.UPDATE_EVENT_RANGES:
                    self.handle_update_event_ranges(actor_id, data)
                elif message == Messages.PROCESS_DONE:
                    self.handle_actor_done(actor_id)
            self.on_tick()
            new_messages, self.actors_message_queue = self.wait_on_messages()

        self._logger.debug("Finished handling the Actors. Raythena will shutdown now.")

    def wait_on_messages(self) -> Tuple[List[ObjectRef], List[ObjectRef]]:
        """
        Wait on part of the pending futures to complete. Wait for 1 second trying to fetch half of the pending futures.
        If no futures are ready, then wait another second to fetch a tenth of the pending futures.
        If there are still no futures ready, then wait for the timeout interval or until one future is ready. If this is the beginning of the job, i.e. no
        events have finished processing yet, then wait forever until one future is ready instead of only timeout interval.

        Returns:
            Tuple of a list of completed futures and a list of pending futures, respectively
        """
        if self.bookKeeper.have_finished_events():
            timeoutinterval = self.timeoutinterval
        else:
            timeoutinterval = None

        messages, queue = ray.wait(
            self.actors_message_queue, num_returns=max(1, len(self.actors_message_queue) // 2), timeout=1)
        if not messages:
            messages, queue = ray.wait(
                self.actors_message_queue, num_returns=max(1, len(self.actors_message_queue) // 10), timeout=1)
        if not messages:
            messages, queue = ray.wait(
                self.actors_message_queue, num_returns=1, timeout=timeoutinterval)
        return messages, queue

    def handle_actor_done(self, actor_id: str) -> bool:
        """
        Handle workers that finished processing a job

        Args:
            actor_id: actor which finished processing a job

        Returns:
            True if more jobs should be sent to the actor
        """
        # try to assign a new job to the actor
        has_jobs = self.bookKeeper.has_jobs_ready()
        # TODO: Temporary hack
        has_jobs = False
        if has_jobs:
            self.enqueue_actor_call(actor_id, self[actor_id].mark_new_job.remote())
        else:
            self.terminated.append(actor_id)
            self.bookKeeper.process_actor_end(actor_id)
            self._logger.info(f"{actor_id} stopped")
            # do not get new messages from this actor
        return has_jobs

    def handle_update_event_ranges(self, actor_id: str, data: EventRangeUpdate) -> None:
        """
        Handle worker update event ranges

        Args:
            actor_id: worker sending an event update
            data: event update

        Returns:
            None
        """
        self.bookKeeper.process_event_ranges_update(actor_id, data)
        self.enqueue_actor_call(actor_id, self[actor_id].get_message.remote())

    def handle_update_job(self, actor_id: str, data: Any) -> None:
        """
        Handle worker job update. This is currently ignored and we simply get a new message from the actor.

        Args:
            actor_id: worker sending the job update
            data: job update

        Returns:
            None
        """
        self.enqueue_actor_call(actor_id, self[actor_id].get_message.remote())

    def handle_request_event_ranges(self, actor_id: str, data: EventRangeRequest, total_sent: int) -> int:
        """
        Handle event ranges request. Event ranges are distributed evenly amongst workers,
        the number of events returned in a single request is capped to the number of local events
        divided by the number of actors. This cap is updated every time new events are retrieved from Harvester.

        If the driver doesn't have enough events to send to the actor, then it will initiate or wait on a pending event request to Harvester to get more events.
        It will only return less events than the request number (or cap) if Harvester returns no events.
        Requests to Harvester are skipped if it was flagged as not having any events left for the current actor's job.

        Args:
            actor_id: worker sending the event ranges update
            data: event range request
            total_sent: number of ranges already sent by the driver to all actors

        Returns:
            Update number of ranges sent to actors
        """
        panda_id = self.bookKeeper.get_actor_job(actor_id)

        # get the min between requested ranges and what is available for each actor
        n_ranges = min(data[panda_id]['nRanges'], self.available_events_per_actor)

        evt_range = self.bookKeeper.fetch_event_ranges(
            actor_id, n_ranges)
        # did not fetch enough events and harvester might have more, needs to get more events now
        # while (len(evt_range) < n_ranges and
        #        not self.bookKeeper.is_flagged_no_more_events(
        #            panda_id)):
        #     # self.request_event_ranges(block=True)
        #     n_ranges = max(0, min(data[panda_id]['nRanges'], self.available_events_per_actor) - len(evt_range))
        #     evt_range += self.bookKeeper.fetch_event_ranges(
        #         actor_id, n_ranges)
        if evt_range:
            total_sent += len(evt_range)
        self.enqueue_actor_call(actor_id, self[actor_id].receive_event_ranges.remote(
            Messages.REPLY_OK if evt_range else
            Messages.REPLY_NO_MORE_EVENT_RANGES, evt_range))
        self._logger.info(f"Sending {len(evt_range)} events to {actor_id}")
        return total_sent

    def handle_job_request(self, actor_id: str) -> None:
        """
        Handle worker job request
        Args:
            actor_id: worker requesting a job

        Returns:
            None
        """
        job = self.bookKeeper.assign_job_to_actor(actor_id)
        if not job:
            self._logger.warn(f"Could not assign a job to {actor_id}")
            return
            # self.request_event_ranges(block=True)
            # job = self.bookKeeper.assign_job_to_actor(actor_id)

        self.enqueue_actor_call(actor_id, self[actor_id].receive_job.remote(Messages.REPLY_OK
                                if job else Messages.REPLY_NO_MORE_JOBS, self.remote_jobdef_byid[job['PandaID']]))

    def request_event_ranges(self, block: bool = False) -> None:
        """
        If no event range request is ongoing, checks if any jobs needs more ranges, and if so,
        sends a request to harvester. If an event range request has been sent (including one from
        the same call of this function), checks if a reply is available. If so, add the ranges to the bookKeeper.
        If block == true and there are requests in-flight, blocks until a reply is received.

        The current implementation limits the number of in-flight requests to 1.

        Args:
            block: wait on the response from harvester communicator if true,
            otherwise try to fetch response if available

        Returns:
            None
        """
        if self.n_eventsrequest == 0:
            event_request = EventRangeRequest()
            for pandaID in self.bookKeeper.jobs:
                if self.bookKeeper.is_flagged_no_more_events(pandaID):
                    continue
                n_available_ranges = self.bookKeeper.n_ready(pandaID)
                job = self.bookKeeper.jobs[pandaID]
                if n_available_ranges < self.events_cache_size:
                    event_request.add_event_request(pandaID,
                                                    self.events_cache_size,
                                                    job['taskID'],
                                                    job['jobsetID'])

            if len(event_request) > 0:
                self._logger.debug(f"Sending event ranges request to harvester for {self.events_cache_size} events")
                self.requests_queue.put(event_request)
                self.n_eventsrequest += 1

        if self.n_eventsrequest > 0:
            try:
                ranges = self.event_ranges_queue.get(block)
                n_received_events = 0
                for pandaID, ranges_list in ranges.items():
                    n_received_events += len(ranges_list)
                    self._logger.debug(f"got event ranges for job {pandaID}: {len(ranges_list)}")
                if self.first_event_range_request:
                    self.first_event_range_request = False
                    if n_received_events == 0:
                        self.stop()
                self.bookKeeper.add_event_ranges(ranges)
                # TODO do not reference pandaID befoore checking if non-null
                self.available_events_per_actor = max(1, ceil(self.bookKeeper.n_ready(pandaID) / self.n_actors))
                self.n_eventsrequest -= 1
            except Empty:
                pass

    def on_tick(self) -> None:
        """
        Performs upkeep actions that should be executed regularly, after handling a batch of actor messages.

        Returns:
            None
        """
        # self.get_tar_results()
        # self.tar_es_output()
        self.handle_merge_transforms()
        # self.request_event_ranges()

    def cleanup(self) -> None:
        """
        Notify each worker that it should terminate then wait for actor to acknowledge
        that the interruption was received

        Returns:
            None
        """
        handles = list()
        for name, handle in self.actors.items():
            if name not in self.terminated:
                handles.append(handle.interrupt.remote())
                self.terminated.append(name)
        ray.get(handles)

    def run(self) -> None:
        """
        Method used to start the driver, initializing actors, retrieving initial job and event ranges,
        creates job subdir then handle actors until they are all done or stop() has been called
        This function will also create a directory in config.ray.workdir for the retrieved job
        with the directory name being the PandaID. Workers will then each create their own subdirectory in that job directory.

        Returns:
            None
        """
        # gets initial jobs and send an eventranges request for each jobs
        jobs = self.jobs_queue.get()
        if not jobs:
            self._logger.critical("No jobs provided by communicator, stopping...")
            return
        if len(jobs) > 1:
            self._logger.critical("Raythena can only handle one job")
            return
        self._logger.debug("Adding job and generating event ranges...")
        self.bookKeeper.add_jobs(jobs)
        self.panda_taskid = list(jobs.values())[0]["taskID"]

        # sends an initial event range request
        # self.request_event_ranges(block=True)
        if not self.bookKeeper.has_jobs_ready():
            # self.cpu_monitor.stop()
            self.bookKeeper.stop_cleaner_thread()
            self.bookKeeper.stop_saver_thread()
            self.communicator.stop()
            self._logger.critical("Couldn't fetch a job with event ranges, stopping...")
            return
        total_events = self.bookKeeper.n_ready(self.bookKeeper.jobs.next_job_id_to_process())
        if total_events:
            self.available_events_per_actor = max(1, ceil(total_events / self.n_actors))
            for pandaID in self.bookKeeper.jobs:
                cjob = self.bookKeeper.jobs[pandaID]
                os.makedirs(
                    os.path.join(self.config.ray['workdir'], cjob['PandaID']))
                self.remote_jobdef_byid[pandaID] = ray.put(cjob)

            self.create_actors()

            self.start_actors()
            # self.request_event_ranges()
            try:
                self.handle_actors()
            except Exception as e:
                self._logger.error(f"{traceback.format_exc()}")
                self._logger.error(f"Error while handling actors: {e}. stopping...")

            if self.config.logging.get('copyraylogs', False):
                ray_logs = os.path.join(self.workdir, "ray_logs")
                try:
                    shutil.copytree(self.session_log_dir, ray_logs)
                except Exception as e:
                    self._logger.error(f"Failed to copy ray logs to workdir: {e}")
        else:
            self._logger.info("No events to process, check for remaining merge jobs...")
        self._logger.debug("Waiting on merge transforms")
        # Workers might have sent event ranges update since last check, create possible merge jobs
        self.bookKeeper.stop_saver_thread()
        self.handle_merge_transforms(True)
        self.bookKeeper.stop_cleaner_thread()
        # need to explicitely save as we stopped saver_thread
        self.bookKeeper.save_status()
        self.communicator.stop()
        # self.cpu_monitor.stop()
        self.bookKeeper.print_status()
        self._logger.debug("All driver threads stopped. Quitting...")

    def stop(self) -> None:
        """
        Stop the driver.

        Returns:
            None
        """
        # check for running tar processes?
        self._logger.info("Interrupt received... Graceful shutdown")
        self.running = False
        self.cleanup()

    def handle_actor_exception(self, actor_id: str, ex: Exception) -> None:
        """
        Handle exception that occurred in an actor process. Log the exception and count the number of exceptions
        that were produced by the same actor. If the number of exceptions is greater than the threshold, the driver will simply drop the actor
        by no longer calling remote functions on it.

        Args:
            actor_id: the actor that raised the exception
            ex: exception raised in actor process

        Returns:
            None
        """
        self._logger.warning(f"An exception occurred in {actor_id}: {ex}")
        if actor_id not in self.failed_actor_tasks_count:
            self.failed_actor_tasks_count[actor_id] = 0

        self.failed_actor_tasks_count[actor_id] += 1
        if self.failed_actor_tasks_count[actor_id] < self.max_retries_error_failed_tasks:
            self.enqueue_actor_call(actor_id, self[actor_id].get_message.remote())
            self._logger.warning(f"{actor_id} failed {self.failed_actor_tasks_count[actor_id]} times. Retrying...")
        else:
            self._logger.warning(f"{actor_id} failed too many times. No longer fetching messages from it")
            if actor_id not in self.terminated:
                self.terminated.append(actor_id)

    def handle_merge_transforms(self, wait_for_completion=False) -> None:
        """
        Checks if the bookkeeper has files ready to be merged. If so, subprocesses for merge tasks are started.
        After starting any subprocess, go through all the running subprocess and poll then to check if any completed and report status to the bookkeepr.

        Args:
            wait_for_completion: Wait for all the subprocesses (including those started by this call) to finish before returning
        """
        if self.total_running_merge_transforms >= self.max_running_merge_transforms:
            return
        self.bookKeeper.check_mergeable_files()
        merge_files = self.bookKeeper.get_file_to_merge()
        while merge_files:
            (input_filename, event_ranges) = merge_files
            assert len(event_ranges) > 0
            if input_filename not in self.running_merge_transforms:
                self.running_merge_transforms[input_filename] = dict()
            output_filename = f"{input_filename.replace('EVNT', 'HITS')}-{event_ranges[0][1].eventRangeID.split('-')[-1]}"
            sub_process = self.hits_merge_transform([e[0] for e in event_ranges], output_filename)
            self._logger.debug(f"Starting merge transform for {input_filename} : {output_filename}")
            self.running_merge_transforms[input_filename][output_filename] = (event_ranges, sub_process)
            self.total_running_merge_transforms += 1
            if self.total_running_merge_transforms >= self.max_running_merge_transforms:
                break
            merge_files = self.bookKeeper.get_file_to_merge()

        to_remove = []
        for input_filename, processes_for_input_file in self.running_merge_transforms.items():
            for output_filename, (event_ranges, sub_process) in processes_for_input_file.items():
                if wait_for_completion:
                    while sub_process.poll() is None:
                        time.sleep(5)
                if sub_process.poll() is not None:
                    to_remove.append((input_filename, output_filename))
                    self.total_running_merge_transforms -= 1
                    if sub_process.returncode == 0:
                        event_ranges_map = {}
                        for (event_range_output, event_range) in event_ranges:
                            event_ranges_map[event_range.eventRangeID] = TaskStatus.build_eventrange_dict(event_range, event_range_output)
                        self.bookKeeper.report_merged_file(self.panda_taskid, input_filename, output_filename, event_ranges_map)
                    else:
                        self._logger.debug(f"Merge transform failed with return code {sub_process.returncode}")
        for (i, o) in to_remove:
            del self.running_merge_transforms[i][o]

    def hits_merge_transform(self, input_files: Iterable[str], output_file: str) -> subprocess.Process:
        """
        Prepare the shell command for the merging subprocess and starts it.

        Args:
            input_files: individual hits files produced by the event service simulation to be merged
            output_file: filename of the merged hits file

        Returns:
            Process: the handle to the subprocess
        """
        if not input_files:
            return
        tmp_dir = tempfile.mkdtemp()
        file_list = " ".join(input_files)
        output_file = os.path.join(self.output_dir, output_file)
        container_script = "if [[ -f /alrb/postATLASReleaseSetup.sh ]]; then source /alrb/postATLASReleaseSetup.sh; fi;"
        container_script += f"HITSMerge_tf.py --inputHITSFile {file_list} --outputHITS_MRGFile {output_file};"

        cmd = str()
        cmd += "export ATLAS_LOCAL_ROOT_BASE=/cvmfs/atlas.cern.ch/repo/ATLASLocalRootBase;"
        cmd += "export thePlatform=\"${SLURM_SPANK_SHIFTER_IMAGEREQUEST}\";"
        cmd += "source ${ATLAS_LOCAL_ROOT_BASE}/user/atlasLocalSetup.sh --swtype shifter -c $thePlatform -d -s none"
        cmd += f" -r \"{container_script}\" -e \"--clearenv\";RETURN_VAL=$?; rm -r {tmp_dir};exit $RETURN_VAL;"
        return Popen(cmd,
                     stdin=DEVNULL,
                     stdout=DEVNULL,
                     stderr=DEVNULL,
                     shell=True,
                     cwd=tmp_dir,
                     close_fds=True)
