import collections
from functools import reduce
import json
import threading
from raythena.utils.config import Config
from raythena.utils.eventservice import PandaJobQueue, EventRange, PandaJob, EventRangeUpdate, EventRangeDef, JobDef, PilotEventRangeUpdateDef
from raythena.utils.exception import ExThread
from raythena.utils.logging import make_logger

from typing import Deque, Dict, Set, Optional, List, Mapping, Sequence, Union, Tuple, Any

import time
import os
import re


class TaskStatus:
    """
    Utility class which manages the persistancy to file of the progress on a given Panda task.

    All operations (set_eventrange_simulated, set_eventrange_failed, set_file_merged) are lazy.
    They will only enqueue a message which will only be processed just before writting the status to disk in save_status.
    The reason for this design is that save_status and the update operations are supposed to be called by different threads and would
    therefore add synchronization overhead and latency for the main driver thread responsible for polling actors. Having a single thread
    updating and serializing the dictionary eliminate the need for synchronization however it also means that other thread reading the dictionary
    (e.g. from get_nsimulated) will get out of date information as there will most likely be update pending in the queue at any point in time
    """

    SIMULATED = "simulated"
    MERGED = "merged"
    FAILED = "failed"

    def __init__(self, job: PandaJob, config: Config) -> None:
        self.config = config
        self.job = job
        self._logger = make_logger(self.config, "TaskStatus")
        self.output_dir = config.ray.get("outputdir")
        self.filepath = os.path.join(self.output_dir, f"{self.job['taskID']}.json")
        self.tmpfilepath = f"{self.filepath}.tmp"
        self._events_per_file = 500
        self._status: Dict[str, Union[Dict[str, Dict[str, Dict[str, str]]], Dict[str, str]]] = dict()
        self._update_queue: Deque[Tuple[str, Union[EventRange, Tuple]]] = collections.deque()
        self._restore_status()

    def _default_init_status(self):
        """
        Default initialization of the status dict
        """
        self._status[TaskStatus.SIMULATED] = dict()
        self._status[TaskStatus.MERGED] = dict()
        self._status[TaskStatus.FAILED] = dict()

    def _restore_status(self):
        """
        Tries to restore the previously saved status by reading the file written by save_status().
        If it fails to load data from the status file, try to load data from the potential temporary file
        """
        filename = self.filepath
        if not os.path.isfile(filename):
            if not os.path.isfile(self.tmpfilepath):
                # no savefile, init dict
                self._default_init_status()
                return
            else:
                filename = self.tmpfilepath

        try:
            with open(filename, 'r') as f:
                self._status = json.load(f)
        except OSError as e:
            # failed to load status, try to read from a possible tmp file if it exists and not already done
            if filename != self.tmpfilepath and os.path.isfile(self.tmpfilepath):
                try:
                    with open(self.tmpfilepath, 'r') as f:
                        self._status = json.load(f)
                except OSError as ee:
                    self._logger.error(e.strerror)
                    self._logger.error(ee.strerror)
                    self._default_init_status()

    def save_status(self, write_to_tmp=True):
        """
        Save the current status to a json file. Before saving to file, the update queue will be drained, actually carrying out the operations to the dictionary
        that will be written to file.

        Args:
            write_to_tmp: if true, the json data will be written to a temporary file then renamed to the final file
        """

        # dequeue is empty, nothing new to save
        if not self._update_queue:
            return

        # Drain the update deque, actually applying update to the status dictionnary
        while self._update_queue:
            operation_type, data = self._update_queue.popleft()
            if operation_type == TaskStatus.SIMULATED:
                self._set_eventrange_simulated(*data)
            elif operation_type == TaskStatus.MERGED:
                self._set_file_merged(*data)
            elif operation_type == TaskStatus.FAILED:
                self._set_eventrange_failed(data)

        filename = self.filepath
        if write_to_tmp:
            filename = self.tmpfilepath
        try:
            with open(filename, 'w') as f:
                json.dump(self._status, f)

            if write_to_tmp:
                os.replace(filename, self.filepath)
        except OSError as e:
            self._logger.error(f"Failed to save task status: {e.strerror}")

    def _build_eventrange_dict(self, eventrange: EventRange, output_file: str = None) -> Dict[str, Any]:
        """
        Takes an EventRange object and retuns the dict representation which should be saved in the state file

        Args:
            eventrange: the eventrange to convert
        Returns:
            The dictionnary to serialize
        """
        res = {"eventRangeID": eventrange.eventRangeID, "startEvent": eventrange.startEvent, "lastEvent": eventrange.lastEvent}
        if output_file:
            res["path"] = output_file
        return res

    def set_eventrange_simulated(self, eventrange: EventRange, simulation_output_file: str):
        """
        Enqueue a message indicating that an event range has been simulated

        Args:
            eventrange: the event range
        """
        self._update_queue.append((TaskStatus.SIMULATED, (eventrange, simulation_output_file)))

    def _set_eventrange_simulated(self, eventrange: EventRange, simulation_output_file: str):
        """
        Performs the update of the internal dictionnary of a simulated event range

        Args:
            eventrange: the event range
        """
        filename = eventrange.PFN
        simulated_dict = self._status[TaskStatus.SIMULATED]
        if filename not in simulated_dict:
            simulated_dict[filename] = dict()
        simulated_dict[filename][eventrange.eventRangeID] = self._build_eventrange_dict(eventrange, simulation_output_file)

    def set_file_merged(self, inputfile: str, outputfile: str):
        """
        Enqueue a message indicating that a file has been merged.

        Args:
            eventrange: the event range
        """
        self._update_queue.append((TaskStatus.MERGED, (inputfile, outputfile)))

    def _set_file_merged(self, inputfile: str, outputfile: str):
        """
        Performs the update of the internal dictionnary of a merged file

        Args:
            inputfile: the path to the corresponding input file
            outputfile: the path to the merged file
        """

        # unlikely but we could have a file with only failed events
        # do not delete entry of failed event ranges to keep track of it
        if inputfile in self._status[TaskStatus.SIMULATED]:
            del self._status[TaskStatus.SIMULATED][inputfile]
        self._status[TaskStatus.MERGED][inputfile] = outputfile

    def set_eventrange_failed(self, eventrange: EventRange):
        """
        Enqueue a message indicating that an event range has failed.

        Args:
            eventrange: the event range
        """
        self._update_queue.append((TaskStatus.FAILED, eventrange))

    def _set_eventrange_failed(self, eventrange: EventRange):
        """
        Performs the update of the internal dictionnary of a failed event range

        Args:
            eventrange: the event range
        """
        filename = eventrange.PFN
        failed_dict = self._status[TaskStatus.FAILED]
        if filename not in failed_dict:
            failed_dict[filename] = dict()
        failed_dict[filename][eventrange.eventRangeID] = self._build_eventrange_dict(eventrange)

    def get_nsimulated(self, filename=None) -> int:
        """
        Total number of event ranges that have been simulated but not yet merged.

        Args:
            filename: if none, returns the total number of simulated events. If specified, returns the number of events simulated for that specific file

        Returns:
            the number of events simulated
        """
        if filename:
            return len(self._status[TaskStatus.SIMULATED].get(filename, []))
        return reduce(lambda acc, cur: acc + len(cur), self._status[TaskStatus.SIMULATED].values(), 0)

    def get_nfailed(self, filename=None) -> int:
        """
        Total number of event ranges that have failed.

        Args:
            filename: if none, returns the total number of failed events. If specified, returns the number of events failed for that specific file

        Returns:
            the number of events failed
        """
        if filename:
            return len(self._status[TaskStatus.FAILED].get(filename, []))
        return reduce(lambda acc, cur: acc + len(cur), self._status[TaskStatus.FAILED].values(), 0)

    def get_nmerged(self, filename=None) -> int:
        """
        Total number of event ranges that have been merged.

        Args:
            filename: if none, returns the total number of merged events. If specified,
            returns the number of events merged for that specific file which should be constant

        Returns:
            the number of events merged
        """
        if filename in self._status[TaskStatus.MERGED]:
            return self._events_per_file  # TODO: update with nevents per file provided by Harvester
        return len(self._status[TaskStatus.MERGED]) * self._events_per_file  # TODO: update with nevents per file provided by Harvester


class BookKeeper(object):
    """
    Performs bookkeeping of jobs and event ranges distributed to workers
    """

    def __init__(self, config: Config) -> None:
        self.jobs: PandaJobQueue = PandaJobQueue()
        self.config: Config = config
        self.output_dir = config.ray.get("outputdir")
        self._logger = make_logger(self.config, "BookKeeper")
        self.actors: Dict[str, Optional[str]] = dict()
        self.rangesID_by_actor: Dict[str, Set[str]] = dict()
        self.files_ready_to_merge: Dict[str, List[str]] = dict()
        self.last_status_print = time.time()
        self.taskstatus: Dict[str, TaskStatus] = dict()
        self.stop_event = threading.Event()
        self.save_state_thread = ExThread(target=self._saver_thead_run, name="status-saver-thread")

    def _saver_thead_run(self):

        while not self.stop_event.is_set():
            for task_status in self.taskstatus.values():
                task_status.save_status()
            self.check_mergeable_files()
            # wait for 60s before next update or until the stop condition is met
            self.stop_event.wait(60.0)

        # Perform a last drain of pending update before stopping
        for task_status in self.taskstatus.values():
            task_status.save_status()
        self.check_mergeable_files()

    def check_mergeable_files(self):
        """
        Goes through the current task status, checks if a file has been entierly processed (event ranges all simulated or failed) and
        if so adds the file to self.files_ready_to_merge
        """
        empty = []
        for task_status in self.taskstatus.values():
            simulated = task_status._status[TaskStatus.SIMULATED]
            failed = task_status._status[TaskStatus.FAILED]
            for input_file in simulated:
                if input_file in self.files_ready_to_merge:
                    continue
                if len(simulated[input_file]) + len(failed.get(input_file, empty)) == 500:
                    self.files_ready_to_merge[input_file] = list(map(lambda r: r["path"], simulated[input_file].values()))

    def stop_save_thread(self):
        """
        Stop and join the thread writing task status to disk and prepare a new thread for execution.
        """
        self.stop_event.set()
        self.save_state_thread.join()
        self.save_state_thread = ExThread(target=self._saver_thead_run, name="status-saver-thread")

    def start_save_thread(self):
        """
        Start the thread responsible for writing task status to disk
        """
        if not self.save_state_thread.is_alive():
            self.stop_event.clear()
            self.save_state_thread.start()

    def add_jobs(self, jobs: Mapping[str, JobDef], start_save_thread=True) -> None:
        """
        Register new jobs. Event service jobs will not be assigned to worker until event ranges are added to the job.
        This will also automatically start the thread responsible for saving the task status to file if the parameter start_save_thread is True.
        If the thread is started, it must be stopped with stop_save_thread before exiting the application

        Args:
            jobs: job dict
            start_save_thread: Automatically starts the thread writing task status to file

        Returns:
            None
        """
        self.jobs.add_jobs(jobs)
        for pandaID in self.jobs:
            job = self.jobs[pandaID]
            if job["taskID"] not in self.taskstatus:
                ts = TaskStatus(job, self.config)
                self.taskstatus[job['taskID']] = ts
                self._generate_event_ranges(job, ts)
        if start_save_thread:
            self.start_save_thread()

    def _generate_event_ranges(self, job: PandaJob, task_status: TaskStatus):
        """
        Generates all the event ranges which still need to be simulated and adds them to the
        EventRangeQueue of the job.

        Args:
            job: the job to which the generated event ranges will be assigned
            task_status: current status of the panda task
        """

        input_evnt_files = re.findall(r"\-\-inputEVNTFile=([\w\.\,]*) \-", job["jobPars"])
        if input_evnt_files:
            guids = job["GUID"].split(',')
            files = input_evnt_files[0].split(',')
            scope = job["scopeIn"]
            event_ranges = []
            merged_files = task_status._status[TaskStatus.MERGED]
            simulated_ranges = task_status._status[TaskStatus.SIMULATED]
            failed_ranges = task_status._status[TaskStatus.FAILED]
            for file, guid in zip(files, guids):
                if file in merged_files:
                    continue
                file_simulated_ranges = simulated_ranges.get(file)
                file_failed_ranges = failed_ranges.get(file)
                # TODO: get actual # of event ranges per file from Harvester
                for i in range(1, 501):
                    range_id = f"{file}-{i}"
                    if file_failed_ranges and range_id in file_failed_ranges or file_simulated_ranges and range_id in file_simulated_ranges:
                        continue
                    else:
                        event_range = EventRange(range_id, i, i, file, guid, scope)
                        event_ranges.append(event_range)
            job.event_ranges_queue.add_new_event_ranges(event_ranges)

    def add_event_ranges(
            self, event_ranges: Mapping[str, Sequence[EventRangeDef]]) -> None:
        """
        Assign event ranges to the jobs in queue.

        Args:
            event_ranges: List of event ranges dict as returned by harvester

        Returns:
            None
        """
        self.jobs.process_event_ranges_reply(event_ranges)

    def have_finished_events(self) -> bool:
        """
        Checks if any job finished any events

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

    def get_actor_job(self, actor_id: str) -> Optional[str]:
        """
        Get the job ID for the given actor ID

        Args:
            actor_id: actor ID

        Returns:
            job ID if the actor is assigned to a job, None otherwise
        """
        return self.actors.get(actor_id, None)

    def assign_job_to_actor(self, actor_id: str) -> Optional[PandaJob]:
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
        Retrieve event ranges for an actor. The specified actor should have a job assigned from assign_job_to_actor() or an empty list will be returned.
        If the job assigned to the actor doesn't have enough range currently available, it will assign all of its remaining anges
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

    def report_merged_file(self, taskID: str, merged_input_file: str, merged_output_file: str):
        if merged_input_file in self.files_ready_to_merge:
            del self.files_ready_to_merge[merged_input_file]
        self.taskstatus[taskID].set_file_merged(merged_input_file, merged_output_file)

    def process_event_ranges_update(
        self, actor_id: str, event_ranges_update: Union[Sequence[PilotEventRangeUpdateDef], EventRangeUpdate]
    ) -> Optional[Tuple[EventRangeUpdate, EventRangeUpdate]]:
        """
        Process the event ranges update sent by the worker. This will update the status of event ranges in the update as well as building
        the list of event ranges to be tarred up for each input file.

        Args:
            actor_id: actor worker_id that sent the update
            event_ranges_update: range update sent by the payload, i.e. pilot

        Returns:
            A tuple with two EventRangeUpdate object, the first one contains event ranges in status DONE,
            the second one contains event ranges in status FAILED or FATAL
        """
        panda_id = self.actors.get(actor_id, None)
        if not panda_id:
            return

        if not isinstance(event_ranges_update, EventRangeUpdate):
            event_ranges_update = EventRangeUpdate.build_from_dict(
                panda_id, event_ranges_update)
        self.jobs.process_event_ranges_update(event_ranges_update)
        task_status = self.taskstatus[self.jobs[panda_id]['taskID']]
        job_ranges = self.jobs.get_event_ranges(panda_id)
        actor_ranges = self.rangesID_by_actor[actor_id]
        failed_events_list = []
        failed_events = {panda_id: failed_events_list}
        for r in event_ranges_update[panda_id]:
            if 'eventRangeID' in r and r['eventRangeID'] in actor_ranges:
                range_id = r['eventRangeID']
                actor_ranges.remove(range_id)
                if r['eventStatus'] == EventRange.DONE:
                    task_status.set_eventrange_simulated(job_ranges[range_id], r['path'])
                elif r['eventStatus'] in [EventRange.FAILED, EventRange.FATAL]:
                    self._logger.info(f"Received failed event from {actor_id}: {r}")
                    task_status.set_eventrange_failed(job_ranges[range_id])
                    failed_events_list.append(r)
        now = time.time()
        if now - self.last_status_print > 60:
            self.last_status_print = now
            self.print_status()
        failed_events = EventRangeUpdate(failed_events) if failed_events_list else None
        return event_ranges_update, failed_events

    def print_status(self) -> None:
        """
        Print a status of each job
        """
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
        which did not receive an update are marked as available again.

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
        Checks if a job can still receive more event ranges from harvester.
        This function returning Trued doesn't guarantee that Harvester has more events available,
        only that it may or may not have more events available. If false is returned, Harvester doesn't have more events available

        Args:
            panda_id: job worker_id to check

        Returns:
            True if more event ranges requests may be retrieved from harvester for the specified job, False otherwise
        """
        return self.jobs[panda_id].no_more_ranges
