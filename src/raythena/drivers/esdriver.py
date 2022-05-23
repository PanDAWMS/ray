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
from typing import Any, Dict, Iterator, List, Set, Tuple, Union

import ray
from ray.exceptions import RayActorError
from raythena.actors.esworker import ESWorker
from raythena.utils.logging import disable_stdout_logging, log_to_file, make_logger
from raythena.drivers.baseDriver import BaseDriver
from raythena.drivers.communicators.baseCommunicator import BaseCommunicator
from raythena.utils.config import Config
from raythena.drivers.communicators.harvesterFileMessenger import HarvesterFileCommunicator
from raythena.utils.eventservice import (EventRange, EventRangeRequest,
                                         EventRangeUpdate, JobReport, Messages,
                                         PandaJob, PandaJobQueue,
                                         PandaJobRequest)
from raythena.utils.exception import BaseRaythenaException
from raythena.utils.ray import build_nodes_resource_list

# from raythena.utils.timing import CPUMonitor

EventRangeTypeHint = Dict[str, str]
PandaJobTypeHint = Dict[str, str]


class BookKeeper(object):
    """
    Performs bookkeeping of jobs and event ranges distributed to workers
    """

    def __init__(self, config: Config) -> None:
        self.jobs = PandaJobQueue()
        self.config = config
        self._logger = make_logger(self.config, "BookKeeper")
        self.actors: Dict[str, Union[str, None]] = dict()
        self.rangesID_by_actor: Dict[str, Set[str]] = dict()
        self.finished_range_by_input_file: Dict[str, List[Dict]] = dict()
        self.ranges_to_tar_by_input_file: Dict[str, List[Dict]] = dict()
        self.ranges_to_tar: List[List[Dict]] = list()
        self.ranges_tarred_up: List[List[Dict]] = list()
        self.ranges_tarred_by_output_file: Dict[str, List[Dict]] = dict()
        self.start_time = time.time()
        self.tarmaxfilesize = self.config.ray['tarmaxfilesize']
        self.last_status_print = time.time()

    def get_ranges_to_tar(self) -> List[List[Dict]]:
        """
        Return a list of lists of event Ranges to be written to tar files

        Args:
            None

        Returns:
            List of Lists of Event Ranges to be put into tar files
        """
        return self.ranges_to_tar

    def get_ranges_to_tar_by_input_file(self) -> Dict[str, List[Dict]]:
        """
        Return the dictionary of event Ranges to be written to tar files organized by input files

        Args:
            None

        Returns:
            dict of Event Ranges organized by input file
        """
        return self.ranges_to_tar_by_input_file

    def create_ranges_to_tar(self) -> bool:
        """
        using the event ranges organized by input file in ranges_to_tar_by_input_file
        loop over the entries creating a list of lists which container all of event ranges to be tarred up.
        update the dictionary of event Ranges to be written to tar files organized by input files
        removing the event ranges event Range lists organized by input files

        Args:
             None:

        Returns:
           True if there are any ranges to tar up. False otherwise
        """
        return_val = False
        # loop over input file names and process the list
        try:
            self.ranges_to_tar = []
            for input_file in self.ranges_to_tar_by_input_file:
                total_file_size = 0
                file_list = []
                while self.ranges_to_tar_by_input_file[input_file]:
                    event_range = self.ranges_to_tar_by_input_file[input_file].pop()
                    if event_range["fsize"] > self.tarmaxfilesize:
                        # if an event is larger than max tar size, tar it alone
                        self.ranges_to_tar.append([event_range])
                    elif total_file_size + event_range['fsize'] > self.tarmaxfilesize:
                        # reached the size limit
                        self.ranges_to_tar_by_input_file[input_file].append(event_range)
                        self.ranges_to_tar.append(file_list)
                        total_file_size = 0
                        file_list = []
                    else:
                        total_file_size = total_file_size + event_range['fsize']
                        file_list.append(event_range)
                if len(file_list) > 0:
                    self.ranges_to_tar.append(file_list)
            if len(self.ranges_to_tar) > 0:
                return_val = True
        except Exception:
            self._logger.debug("create_ranges_to_tar - can not create list of ranges to tar")
            return_val = False
        return return_val

    def add_jobs(self, jobs: Dict[str, PandaJobTypeHint]) -> None:
        """
        Register new jobs. Event service jobs will not be assigned to worker until event ranges are added to the job

        Args:
            jobs: job dict

        Returns:
            None
        """
        self.jobs.add_jobs(jobs)

    def add_event_ranges(
            self, event_ranges: Dict[str, List[EventRangeTypeHint]]) -> None:
        """
        Assign event ranges to jobs in queue.

        Args:
            event_ranges: event ranges dict as returned by harvester

        Returns:
            None
        """
        self.jobs.process_event_ranges_reply(event_ranges)

    def have_finished_events(self) -> bool:
        """
        Checks if any job finished any events

        Args:
            None

        Returns:
            True if any event ranges requests have finished, False otherwise
        """
        nfinished = 0
        for pandaID in self.jobs:
            job_ranges = self.jobs.get_event_ranges(pandaID)
            nfinished = nfinished + job_ranges.nranges_done()
        return nfinished > 0

    def has_jobs_ready(self) -> bool:
        """
        Checks if a job can be assigned to a worker

        Returns:
            True if a job is ready to be processed by a worker
        """
        job_id = self.jobs.next_job_id_to_process()
        return job_id is not None

    def get_actor_job(self, actor_id: str):
        return self.actors.get(actor_id, None)

    def assign_job_to_actor(self, actor_id: str) -> Union[PandaJob, None]:
        """
        Retrieve a job from the job queue to be assigned to a worker

        Args:
            actor_id: actor to which the job should be assigned to

        Returns:
            job worker_id of assigned job, None if no job is available
        """
        job_id = self.jobs.next_job_id_to_process()
        if job_id:
            self.actors[actor_id] = job_id
        return self.jobs[job_id] if job_id else None

    def fetch_event_ranges(self, actor_id: str, n: int) -> List[EventRange]:
        """
        Retrieve event ranges for an actor. The specified actor should have a job assigned from assign_job_to_actor().
        If the job assigned to the actor doesn't have enough range currently available, it will assign all of its ranges
        to the worker without trying to get new ranges from harvester.

        Args:
            actor_id: actor requesting event ranges
            n: number of event ranges to assign to the actor

        Returns:
            A list of event ranges to be processed by the actor
        """
        if actor_id not in self.actors or not self.actors[actor_id]:
            return list()
        if actor_id not in self.rangesID_by_actor:
            self.rangesID_by_actor[actor_id] = set()
        ranges = self.jobs.get_event_ranges(
            self.actors[actor_id]).get_next_ranges(n)
        self.rangesID_by_actor[actor_id].update(map(lambda e: e.eventRangeID, ranges))
        return ranges

    def process_event_ranges_update(
        self, actor_id: str, event_ranges_update: Union[dict, EventRangeUpdate]
    ) -> Union[Tuple[EventRangeUpdate, EventRangeUpdate], None]:
        """
        Update the event ranges status according to the range update.

        Args:
            actor_id: actor worker_id that sent the update
            event_ranges_update: range update sent by the payload

        Returns:
            None
        """
        panda_id = self.actors.get(actor_id, None)
        if not panda_id:
            return

        if not isinstance(event_ranges_update, EventRangeUpdate):
            event_ranges_update = EventRangeUpdate.build_from_dict(
                panda_id, event_ranges_update)
        self.jobs.process_event_ranges_update(event_ranges_update)
        job_ranges = self.jobs.get_event_ranges(panda_id)
        actor_ranges = self.rangesID_by_actor[actor_id]
        failed_events_list = []
        failed_events = {panda_id: failed_events_list}
        for r in event_ranges_update[panda_id]:
            if 'eventRangeID' in r and r['eventRangeID'] in actor_ranges:
                range_id = r['eventRangeID']
                actor_ranges.remove(range_id)
                if r['eventStatus'] == EventRange.DONE:
                    event_range = job_ranges[range_id]
                    file_basename = os.path.basename(event_range.PFN)
                    if file_basename not in self.finished_range_by_input_file:
                        self.finished_range_by_input_file[file_basename] = list()
                    if file_basename not in self.ranges_to_tar_by_input_file:
                        self.ranges_to_tar_by_input_file[file_basename] = list()
                    self.finished_range_by_input_file[file_basename].append(r)
                    r['PanDAID'] = panda_id
                    self.ranges_to_tar_by_input_file[file_basename].append(r)
                elif r['eventStatus'] in [EventRange.FAILED, EventRange.FATAL]:
                    self._logger.info(f"Received failed event from {actor_id}: {r}")
                    failed_events_list.append(r)
        now = time.time()
        if now - self.last_status_print > 60:
            self.last_status_print = now
            self.print_status()
        failed_events = EventRangeUpdate(failed_events) if failed_events_list else None
        return event_ranges_update, failed_events

    def print_status(self) -> None:
        for panda_id in self.jobs:
            job_ranges = self.jobs.get_event_ranges(panda_id)
            if not job_ranges:
                continue
            message = f"Event ranges status for job {panda_id}:"
            if job_ranges.nranges_available():
                message = f"{message} Ready: {job_ranges.nranges_available()}"
            if job_ranges.nranges_assigned():
                message = f"{message} Assigned: {job_ranges.nranges_assigned()}"
            if job_ranges.nranges_failed():
                message = f"{message} Failed: {job_ranges.nranges_failed()}"
            if job_ranges.nranges_done():
                message = f"{message} Finished: {job_ranges.nranges_done()}"
            self._logger.info(message)

    def process_actor_end(self, actor_id: str) -> None:
        """
        Performs clean-up of event ranges when an actor ends. Event ranges still assigned to this actor
        that did not receive an update are marked as available again

        Args:
            actor_id: worker_id of actor that ended

        Returns:
            None
        """
        panda_id = self.actors.get(actor_id, None)
        if not panda_id:
            return
        actor_ranges = self.rangesID_by_actor.get(actor_id, None)
        if not actor_ranges:
            return
        self._logger.info(f"{actor_id} finished with {len(actor_ranges)} events remaining to process")
        for rangeID in actor_ranges:
            self.jobs.get_event_ranges(panda_id).update_range_state(
                rangeID, EventRange.READY)
        actor_ranges.clear()
        self.actors[actor_id] = None

    def n_ready(self, panda_id: str) -> int:
        """
        Checks how many events can be assigned to workers for a given job.

        Args:
            panda_id: job worker_id to check

        Returns:
            Number of ranges that can be assigned to a worker
        """
        return self.jobs.get_event_ranges(panda_id).nranges_available()

    def is_flagged_no_more_events(self, panda_id: str) -> bool:
        """
        Checks if a job could potentially receive more event ranges from harvester

        Args:
            panda_id: job worker_id to check

        Returns:
            True if more event ranges requests should be sent to harvester for the specified job, False otherwise
        """
        return self.jobs[panda_id].no_more_ranges


class ESDriver(BaseDriver):
    """
    The driver is managing all the ray workers and handling the communication with Harvester. It keeps tracks of
    which event ranges is assigned to which actor using the BookKeeper, and sends requests for jobs, event ranges
    or event ranges update to harvester by using a communicator plugin. It will regularly poll messages from actors and
    handle the message so that actors can execute jobs that they are attributed.

    The driver is scheduling one actor per node in the ray cluster, except for the ray head node which doesn't execute
    any worker
    """

    def __init__(self, config: Config, session_dir: str) -> None:
        """
        Initialize ray custom resources, logging actor, a bookKeeper instance and other attributes.
        The harvester communicator is also initialized and an initial JobRequest is sent

        Args:
            config: application config
        """
        super().__init__(config, session_dir)
        self.id = "Driver"
        self.config_remote = ray.put(self.config)
        self._logger = make_logger(self.config, self.id)
        self.session_log_dir = os.path.join(self.session_dir, "logs")
        self.nodes = build_nodes_resource_list(self.config, run_actor_on_head=False)

        self.requests_queue = Queue()
        self.jobs_queue = Queue()
        self.event_ranges_queue = Queue()

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

        self._logger.debug(f"Raythena initializing, running Ray {ray.__version__} on {gethostname()}")

        # self.cpu_monitor = CPUMonitor(os.path.join(workdir, "cpu_monitor_driver.json"))
        # self.cpu_monitor.start()

        self.communicator: BaseCommunicator = HarvesterFileCommunicator(self.requests_queue,
                                                                        self.jobs_queue,
                                                                        self.event_ranges_queue,
                                                                        config)
        self.communicator.start()
        self.requests_queue.put(PandaJobRequest())
        self.actors = dict()
        self.actors_message_queue = list()
        self.bookKeeper = BookKeeper(config)
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
        self.tar_merge_es_output_dir = os.path.join(self.workdir, "merge_es_output")
        self.tar_merge_es_files_dir = os.path.join(self.workdir, "merge_es_files")
        self.ranges_to_tar: List[List[Dict]] = list()
        self.running_tar_threads = dict()
        self.processed_event_ranges = dict()
        self.finished_tar_tasks = set()
        self.failed_actor_tasks_count = dict()
        self.available_events_per_actor = 0
        self.total_tar_tasks = 0
        self.remote_jobdef_byid = dict()
        self.tar_executor = concurrent.futures.ThreadPoolExecutor(max_workers=self.tarmaxprocesses)

        # create the output directories if needed
        try:
            if not os.path.isdir(self.tar_merge_es_output_dir):
                os.mkdir(self.tar_merge_es_output_dir)
        except Exception:
            self._logger.warn(f"Exception when creating the {self.tar_merge_es_output_dir}")
            raise
        try:
            if not os.path.isdir(self.tar_merge_es_files_dir):
                os.mkdir(self.tar_merge_es_files_dir)
        except Exception:
            self._logger.warn(f"Exception when creating the {self.tar_merge_es_files_dir}")
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
        Initialize actor communication by performing a first call to get_message()

        Returns:
            None
        """
        self.actors_message_queue += [actor.get_message.remote() for actor in self.actors.values()]

    def create_actors(self) -> None:
        """
        Create actors by using custom resources available in self.nodes

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

    def retrieve_actors_messages(self, ready: list) -> Iterator[Tuple[str, int, object]]:
        try:
            messages = ray.get(ready)
        except Exception:
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
        Main function handling messages from all ray actors.

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

    def wait_on_messages(self) -> Tuple[List, List]:
        messages = list()
        queue = list()
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
        Handle worker that finished processing a job

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

    def handle_update_event_ranges(self, actor_id: str, data: Any) -> None:
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
        Handle worker job update

        Args:
            actor_id: worker sending the job update
            data: job update

        Returns:
            None
        """
        self.actors_message_queue.append(
            self[actor_id].get_message.remote())

    def handle_request_event_ranges(self, actor_id: str, data: Any, total_sent: int) -> int:
        """
        Handle event ranges request
        Args:
            actor_id: worker sending the event ranges update
            data: event range update
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
        while (len(evt_range) < n_ranges and
               not self.bookKeeper.is_flagged_no_more_events(
                   panda_id)):
            self.request_event_ranges(block=True)
            n_ranges = max(0, min(data[panda_id]['nRanges'], self.available_events_per_actor) - len(evt_range))
            evt_range += self.bookKeeper.fetch_event_ranges(
                actor_id, n_ranges)
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
            self.request_event_ranges(block=True)
            job = self.bookKeeper.assign_job_to_actor(actor_id)

        self.actors_message_queue.append(self[actor_id].receive_job.remote(
            Messages.REPLY_OK
            if job else Messages.REPLY_NO_MORE_JOBS, self.remote_jobdef_byid[job['PandaID']]))

    def request_event_ranges(self, block: bool = False) -> None:
        """
        If no event range request is ongoing, checks if any jobs needs more ranges, and if so,
        sends a request to harvester. If an event range request has been sent (including one from
        the same call of this function), checks if a reply is available. If so, add the ranges to the bookKeeper.
        If block == true and a request has been sent, blocks until a reply is received

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
                self.available_events_per_actor = max(1, ceil(self.bookKeeper.n_ready(pandaID) / self.n_actors))
                self.n_eventsrequest -= 1
            except Empty:
                pass

    def on_tick(self) -> None:
        """
        Performs actions that should be executed regularly, after handling a batch of actor messages.

        Returns:
            None
        """
        self.get_tar_results()
        self.tar_es_output()
        self.request_event_ranges()

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
        with the directory name being the job worker_id

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
        self.request_event_ranges(block=True)
        if not self.bookKeeper.has_jobs_ready():
            # self.cpu_monitor.stop()
            self.communicator.stop()
            self._logger.critical("Couldn't fetch a job with event ranges, stopping...")
            time.sleep(5)
            return

        for pandaID in self.bookKeeper.jobs:
            cjob = self.bookKeeper.jobs[pandaID]
            os.makedirs(
                os.path.join(self.config.ray['workdir'], cjob['PandaID']))
            self.remote_jobdef_byid[pandaID] = ray.put(cjob)

        self.create_actors()

        self.start_actors()
        self.request_event_ranges()
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
        Handle exception that occurred in an actor process

        Args:
            ex: exception raised in actor process

        Returns:
            None
        """
        self._logger.warn(f"An exception occured in {actor_id}: {ex}")
        if actor_id not in self.failed_actor_tasks_count:
            self.failed_actor_tasks_count[actor_id] = 0

        self.failed_actor_tasks_count[actor_id] += 1
        if self.failed_actor_tasks_count[actor_id] < self.max_retries_error_failed_tasks:
            self.actors_message_queue.append(self[actor_id].get_message.remote())
            self._logger.warn(f"{actor_id} failed {self.failed_actor_tasks_count[actor_id]} times. Retrying...")
        else:
            self._logger.warn(f"{actor_id} failed too many times. No longer fetching messages from it")
            if actor_id not in self.terminated:
                self.terminated.append(actor_id)

    def create_tar_file(self, range_list: list) -> Dict[str, List[Dict]]:
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
                            self._logger.warn((f"Could not add event {path} to tar, file does not exists. "
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

    def create_harvester_data(self, PanDA_id: str, file_path: str, file_chksum: str, file_fsize: int, range_list: list) -> Dict[str, List[Dict]]:
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
                self._logger.warn(f"tar_es_output: Exception {exc} when submitting tar subprocess")
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
                            self._logger.warn(f"ERROR duplicate eventRangeID - {eventRangeID}")
                            for path in (self.processed_event_ranges[PanDA_id][eventRangeID]):
                                self._logger.warn(f"ERROR duplicate eventRangeID - {eventRangeID} {path}")
        except Exception as ex:
            self._logger.info(f"check_for_duplicates Caught exception {ex}")
            return_val = False
            pass
        return return_val