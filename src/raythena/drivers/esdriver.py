import concurrent.futures
import os
import shutil
import sys
import tarfile
import time
import traceback
import zlib
from math import ceil
from queue import Empty, Queue
from socket import gethostname
from typing import (Any, Dict, Iterator, List, Mapping, Sequence,
                    Tuple)

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
from raythena.utils.bookkeeper import BookKeeper
from raythena.utils.config import Config
from raythena.utils.eventservice import (EventRangeDef,
                                         EventRangeRequest, EventRangeUpdate,
                                         JobDef, JobReport, Messages,
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

        self.outputdir = os.path.expandvars(self.config.ray.get("outputdir", self.workdir))
        self.config.ray["outputdir"] = self.outputdir
        self.tar_merge_es_output_dir = self.outputdir
        self.tar_merge_es_files_dir = self.outputdir
        # self.cpu_monitor = CPUMonitor(os.path.join(workdir, "cpu_monitor_driver.json"))
        # self.cpu_monitor.start()

        self.communicator: BaseCommunicator = HarvesterFileCommunicator(self.requests_queue,
                                                                        self.jobs_queue,
                                                                        self.event_ranges_queue,
                                                                        self.config)
        self.communicator.start()
        self.requests_queue.put(PandaJobRequest())
        self.actors: Dict[str, ESWorker] = dict()
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
        self.tar_timestamp = time.time()
        self.tarinterval = self.config.ray['tarinterval']
        self.tarmaxprocesses = self.config.ray['tarmaxprocesses']
        self.ranges_to_tar: List[List[EventRangeDef]] = list()
        self.running_tar_threads = dict()
        self.processed_event_ranges = dict()
        self.finished_tar_tasks = set()
        self.failed_actor_tasks_count = dict()
        self.available_events_per_actor = 0
        self.total_tar_tasks = 0
        self.remote_jobdef_byid = dict()
        self.config_remote = ray.put(self.config)
        self.tar_executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.tarmaxprocesses)

        # create the output directories if needed
        try:
            if not os.path.isdir(self.outputdir):
                os.mkdir(self.outputdir)
        except Exception:
            self._logger.warning(f"Exception when creating the {self.outputdir}")
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
        self.actors_message_queue += [actor.get_message.remote() for actor in self.actors.values()]

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
                    self._logger.debug(f"Prefetched job {job['PandaID']} and {len(event_ranges)} event ranges for {actor_id}")

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
                    self._logger.error(f"Caught exception while fetching result from actor: {e}")
                else:
                    yield actor_id, message, data
        else:
            self._logger.debug(f"Start handling messages batch of {len(messages)} actors")
            for actor_id, message, data in messages:
                yield actor_id, message, data

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
                    self.actors_message_queue.append(
                        self[actor_id].get_message.remote())
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
            self.actors_message_queue.append(self[actor_id].mark_new_job.remote())
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
        _, failed_events = self.bookKeeper.process_event_ranges_update(actor_id, data)
        if failed_events:
            self.requests_queue.put(failed_events)
        self.actors_message_queue.append(self[actor_id].get_message.remote())

    def handle_update_job(self, actor_id: str, data: Any) -> None:
        """
        Handle worker job update. This is currently ignored and we simply get a new message from the actor.

        Args:
            actor_id: worker sending the job update
            data: job update

        Returns:
            None
        """
        self.actors_message_queue.append(
            self[actor_id].get_message.remote())

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
        self.actors_message_queue.append(self[actor_id].receive_event_ranges.remote(
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

        self.actors_message_queue.append(self[actor_id].receive_job.remote(
            Messages.REPLY_OK
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
        self.get_tar_results()
        self.tar_es_output()
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
        self.bookKeeper.add_jobs(jobs)

        # sends an initial event range request
        # self.request_event_ranges(block=True)
        if not self.bookKeeper.has_jobs_ready():
            # self.cpu_monitor.stop()
            self.communicator.stop()
            self._logger.critical("Couldn't fetch a job with event ranges, stopping...")
            time.sleep(5)
            return

        self.available_events_per_actor = max(1, ceil(self.bookKeeper.n_ready(self.bookKeeper.jobs.next_job_id_to_process()) / self.n_actors))
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

        self._logger.debug("Starting new tar tasks")
        # Workers might have sent event ranges update since last check, create remaining tasks regardless of tar interval
        self.tar_es_output(True)

        self._logger.debug("Waiting on tar tasks to finish...")
        while len(self.running_tar_threads) > 0:
            self.get_tar_results()
            time.sleep(1)

        self.bookKeeper.stop_save_thread()
        self.requests_queue.put(JobReport())

        self.communicator.stop()
        # self.cpu_monitor.stop()
        self.bookKeeper.print_status()
        time.sleep(5)
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
            self.actors_message_queue.append(self[actor_id].get_message.remote())
            self._logger.warning(f"{actor_id} failed {self.failed_actor_tasks_count[actor_id]} times. Retrying...")
        else:
            self._logger.warning(f"{actor_id} failed too many times. No longer fetching messages from it")
            if actor_id not in self.terminated:
                self.terminated.append(actor_id)

    def create_tar_file(self, range_list: List[EventRangeDef]) -> Dict[str, List[EventRangeDef]]:
        """
        Use input range_list to create tar file and return list of tarred up event ranges and information needed by Harvester

        Args:
            range_list   list of event range dictionaries
        Returns:
            Dictionary - key PanDAid, item list of event ranges with information needed by Harvester
        """
        return_val = dict()
        # test validity of range_list
        if range_list and isinstance(range_list, list) and len(range_list) > 0:
            # read first element in list to build temporary filename
            file_base_name = "panda." + os.path.basename(range_list[0]['path']) + ".zip"
            temp_file_base_name = file_base_name + ".tmpfile"
            temp_file_path = os.path.join(self.tar_merge_es_output_dir, temp_file_base_name)
            file_path = os.path.join(self.tar_merge_es_output_dir, file_base_name)

            # self.tar_merge_es_output_dir zip files
            # self.tar_merge_es_files_dir  es files

            PanDA_id = range_list[0]['PanDAID']

            file_fsize = 0
            try:
                tarred_ranges_list = list()
                # create tar file looping over event ranges
                with tarfile.open(temp_file_path, "w") as tar:
                    for event_range in range_list:
                        path = event_range['path']
                        if os.path.isfile(path):
                            tar.add(path)
                            tarred_ranges_list.append(event_range)
                        else:
                            self._logger.warning((f"Could not add event {path} to tar, file does not exists. "
                                                  f"Event status: {event_range['eventStatus']}"))
                file_fsize = os.path.getsize(temp_file_path)
                # calculate alder32 checksum
                file_chksum = self.calc_adler32(temp_file_path)
                return_val = self.create_harvester_data(PanDA_id, file_path, file_chksum, file_fsize, tarred_ranges_list)
                for event_range in tarred_ranges_list:
                    lfn = os.path.basename(event_range["path"])
                    pfn = os.path.join(self.tar_merge_es_files_dir, lfn)
                    shutil.move(event_range["path"], pfn)
                # rename zip file (move)
                shutil.move(temp_file_path, file_path)
                return return_val
            except Exception:
                raise
        return return_val

    def calc_adler32(self, file_name: str) -> str:
        """
        Calculate adler32 checksum for file

        Args:
           file_name - name of file to calculate checksum
        Return:
           string with Adler 32 checksum
        """
        val = 1
        blockSize = 32 * 1024 * 1024
        with open(file_name, 'rb') as fp:
            while True:
                data = fp.read(blockSize)
                if not data:
                    break
                val = zlib.adler32(data, val)
        if val < 0:
            val += 2 ** 32
        return hex(val)[2:10].zfill(8).lower()

    def create_harvester_data(self, PanDA_id: str, file_path: str, file_chksum: str, file_fsize: int, range_list: list) -> Dict[str, List[EventRangeDef]]:
        """
        create data structure for telling Harvester what files are merged and ready to process

        Args:
             PanDA_id - Panda ID (as string)
             file_path - path on disk to the merged es output "zip" file
             file_chksum - adler32 checksum of file
             file_fsize - file size in bytes
             range_list - list of event ranges in the file
        Return:
             Dictionary with PanDA_id is the key and dictionary containing file info and list  Event range elements
        """
        return_dict = dict()
        return_list = list()
        for event_range in range_list:
            return_list.append(
                {
                    "eventRangeID": event_range['eventRangeID'],
                    "eventStatus": "finished",
                    "path": file_path,
                    "type": "zip_output",
                    "chksum": file_chksum,
                    "fsize": file_fsize
                })
        if return_list:
            return_dict = {PanDA_id: return_list}
        return return_dict

    def tar_es_output(self, skip_time_check = False) -> None:
        """
        Get from bookKeeper the event ranges arraigned by input file than need to put into output tar files

        Args:
            skip_time_check: flag to skip time interval check

        Returns:
            None
        """
        now = time.time()
        if not skip_time_check and int(now - self.tar_timestamp) < self.tarinterval:
            return

        ranges_to_tar = list()
        if self.bookKeeper.create_ranges_to_tar():
            # add new ranges to tar to the list
            ranges_to_tar = self.bookKeeper.get_ranges_to_tar()
            self.tar_timestamp = now
            self.ranges_to_tar.extend(ranges_to_tar)
            try:
                self.running_tar_threads.update({self.tar_executor.submit(self.create_tar_file, range_list): range_list for range_list in self.ranges_to_tar})
                self.total_tar_tasks += len(self.ranges_to_tar)
                self.ranges_to_tar = list()
            except Exception as exc:
                self._logger.warning(f"tar_es_output: Exception {exc} when submitting tar subprocess")
                pass

            self._logger.debug(f"tar_es_output: #tasks in queue : {len(self.running_tar_threads)}, #total tasks submitted since launch: {self.total_tar_tasks}")

    def get_tar_results(self) -> None:
        """
        Checks the self.running_tar_threads dict for the Future objects. if thread is still running let it run otherwise
        get the results of running, check for duplicates and  pass information onto Harvester

        Returns:
            None

        """
        if len(self.running_tar_threads) == 0:
            return
        done, not_done = concurrent.futures.wait(self.running_tar_threads, timeout=0.001, return_when=concurrent.futures.FIRST_COMPLETED)
        final_update = EventRangeUpdate()
        for future in done:
            try:
                self.finished_tar_tasks.add(future)
                result = future.result()
                if result and isinstance(result, dict) and self.check_for_duplicates(result):
                    final_update.merge_update(EventRangeUpdate(result))
                del self.running_tar_threads[future]
            except Exception as ex:
                exc_type, exc_value, exc_traceback = sys.exc_info()
                self._logger.info(f"get_tar_results: Caught exception {ex}")
                self._logger.info(f"get_tar_results: Caught exception {repr(traceback.format_tb(exc_traceback))}")
                pass
                # raise
        if final_update:
            self.requests_queue.put(final_update)
        if len(done):
            self._logger.debug(f"get_tar_results #completed futures - {len(done)} #pending futures - {len(not_done)}")
        return

    def check_for_duplicates(self, tar_results: dict) -> bool:
        """
        Notify each worker that it should terminate then wait for actor to acknowledge
        that the interruption was received

        Args:
            dictionary of event ranges from finished tar jobs
        Returns:
            True if no duplicate eventRanges
        """
        try:
            return_val = True
            if tar_results and isinstance(tar_results, dict):
                # give results to BookKeeper to send to Harvester ????
                # check for duplicates
                for PanDA_id in tar_results:
                    if PanDA_id not in self.processed_event_ranges:
                        self.processed_event_ranges[PanDA_id] = dict()
                    ranges_info = tar_results[PanDA_id]
                    if ranges_info:
                        for rangeInfo in ranges_info:
                            eventRangeID = rangeInfo['eventRangeID']
                            path = rangeInfo['path']
                            if eventRangeID not in self.processed_event_ranges[PanDA_id]:
                                self.processed_event_ranges[PanDA_id][eventRangeID] = list()
                            self.processed_event_ranges[PanDA_id][eventRangeID].append(path)
                    # loop over processed event ranges list the duplicate files
                    for eventRangeID in self.processed_event_ranges[PanDA_id]:
                        if len(self.processed_event_ranges[PanDA_id][eventRangeID]) > 1:
                            # duplicate eventRangeID
                            return_val = False
                            self._logger.warning(f"ERROR duplicate eventRangeID - {eventRangeID}")
                            for path in (self.processed_event_ranges[PanDA_id][eventRangeID]):
                                self._logger.warning(f"ERROR duplicate eventRangeID - {eventRangeID} {path}")
        except Exception as ex:
            self._logger.info(f"check_for_duplicates Caught exception {ex}")
            return_val = False
            pass
        return return_val
