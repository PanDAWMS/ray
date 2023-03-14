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

    Keys set relation of each sub-dictionnary (simulated, merged, failed, merging):
    - merged and merging key sets are disjoints -- when a file has been fully merged, its entry is removed from merging and moved into merged
    - merged and simulated key sets are disjoints -- when a file has been fully merged, it is no longer necessary to keep track of individual event ranges;
      they are removed from simulated
    - merging is a subset of simulated -- it is possible for events from a given file to have been simulated
      but no merge job has completed for that specific file.
    - No specification for relations between failed and other key sets.
    """

    SIMULATED = "simulated"
    MERGED = "merged"
    MERGING = "merging"
    FAILED = "failed"

    def __init__(self, job: PandaJob, config: Config) -> None:
        self.config = config
        self.job = job
        self._logger = make_logger(self.config, "TaskStatus")
        self.output_dir = config.ray.get("outputdir")
        self.filepath = os.path.join(self.output_dir, "state.json")
        self.tmpfilepath = f"{self.filepath}.tmp"
        self._events_per_file = int(job['nEventsPerInputFile'])
        self._hits_per_file = int(job['esmergeSpec']['nEventsPerOutputFile'])
        assert (self._events_per_file % self._hits_per_file == 0) or (
            self._hits_per_file % self._events_per_file == 0), "Expected number of events per input file to be a multiple of number of hits per merged file"
        # if _hits_per_file > _events_per_file, each input file has a single output file
        self._n_output_per_input_file = max(1, self._events_per_file // self._hits_per_file)
        self._status: Dict[str, Union[Dict[str, Dict[str, Dict[str, str]]], Dict[str, List[str]]]] = dict()
        self._update_queue: Deque[Tuple[str, Union[EventRange, Tuple]]] = collections.deque()
        self._restore_status()

    def _default_init_status(self):
        """
        Default initialization of the status dict
        """
        self._status[TaskStatus.SIMULATED] = dict()
        self._status[TaskStatus.MERGING] = dict()
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
                self._logger.debug("No previous state found")
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
            elif operation_type == TaskStatus.MERGING:
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

    @staticmethod
    def build_eventrange_dict(eventrange: EventRange, output_file: str = None) -> Dict[str, Any]:
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
            simulation_output_file: produced file
        """
        self._update_queue.append((TaskStatus.SIMULATED, (eventrange, simulation_output_file)))

    def _set_eventrange_simulated(self, eventrange: EventRange, simulation_output_file: str):
        """
        Performs the update of the internal dictionnary of a simulated event range

        Args:
            eventrange: the event range
            simulation_output_file: produced file
        """
        filename = eventrange.PFN
        simulated_dict = self._status[TaskStatus.SIMULATED]
        if filename not in simulated_dict:
            simulated_dict[filename] = dict()
        simulated_dict[filename][eventrange.eventRangeID] = TaskStatus.build_eventrange_dict(eventrange, simulation_output_file)

    def set_file_merged(self, input_files: List[str], outputfile: str, event_ranges: Mapping[str, Mapping[str, str]]):
        """
        Enqueue a message indicating that a file has been merged.

        Args:
            inputfile: source evnt file
            outputfile: produced merged hits file
            event_ranges: event ranges merged in the outputfile. Map of [event_range_id, [k, v]]
        """
        self._update_queue.append((TaskStatus.MERGING, (input_files, outputfile, event_ranges)))

    def _set_file_merged(self, input_files: List[str], outputfile: str, event_ranges: Mapping[str, Mapping[str, str]]):
        """
        Performs the update of the internal dictionnary of a merged file.

        Args:
            inputfile: source evnt file
            outputfile: produced merged hits file
            event_ranges: event ranges merged in the outputfile
        """
        total_failed = 0
        failed_dict = self._status[TaskStatus.FAILED]
        for file in input_files:
            if file in failed_dict:
                total_failed += len(failed_dict[file])
        assert len(event_ranges) + total_failed == self._hits_per_file, f"Expected {self._hits_per_file} hits in {outputfile}, got {len(event_ranges)}"
        for inputfile in input_files:
            if inputfile not in self._status[TaskStatus.MERGING]:
                self._status[TaskStatus.MERGING][inputfile] = {outputfile: event_ranges}
            else:
                self._status[TaskStatus.MERGING][inputfile][outputfile] = event_ranges

            if len(self._status[TaskStatus.MERGING][inputfile]) == self._n_output_per_input_file:
                merged_dict = dict()
                self._status[TaskStatus.MERGED][inputfile] = merged_dict
                for merged_outputfile in self._status[TaskStatus.MERGING][inputfile].keys():
                    merged_dict[merged_outputfile] = {"path": os.path.join(self.output_dir, merged_outputfile)}
                del self._status[TaskStatus.MERGING][inputfile]
                del self._status[TaskStatus.SIMULATED][inputfile]
            else:
                for event_range_id in event_ranges:
                    del self._status[TaskStatus.SIMULATED][inputfile][event_range_id]

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
        failed_dict[filename][eventrange.eventRangeID] = TaskStatus.build_eventrange_dict(eventrange)
        if eventrange.eventRangeID in self._status[TaskStatus.SIMULATED].get(filename, {}):
            del self._status[TaskStatus.SIMULATED][eventrange.eventRangeID]

    def get_nsimulated(self, filename=None) -> int:
        """
        Total number of event ranges that have been simulated but not yet merged.

        Args:
            filename: if none, returns the total number of simulated events. If specified, returns the number of events simulated for that specific file

        Returns:
            the number of events simulated
        """
        if filename:
            merged = 0
            if filename in self._status[TaskStatus.MERGED]:
                return merged
            elif filename in self._status[TaskStatus.MERGING]:
                merged = len(self._status[TaskStatus.MERGING][filename]) * self._hits_per_file
            return len(self._status[TaskStatus.SIMULATED].get(filename, [])) - merged

        return reduce(lambda acc, cur: acc + len(cur), self._status[TaskStatus.SIMULATED].values(), 0) - \
            reduce(lambda acc, cur: acc + len(cur) * self._hits_per_file, self._status[TaskStatus.MERGING].values(), 0)

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
            return self._events_per_file
        elif filename in self._status[TaskStatus.MERGING]:
            return len(self._status[TaskStatus.MERGING][filename]) * self._hits_per_file
        return len(self._status[TaskStatus.MERGED]) * self._events_per_file + \
            reduce(lambda acc, cur: acc + len(cur) * self._hits_per_file, self._status[TaskStatus.MERGING].values(), 0)


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
        #  Output files for which we are ready to launch a merge transform
        self.files_ready_to_merge: Dict[str, List[Tuple[str, EventRange]]] = dict()
        # Event ranges for a given input file which have been simulated and a ready to be merged
        self.ranges_to_merge: Dict[str, List[Tuple[str, EventRange]]] = dict()
        # Accumulate event ranges of different input files into the same output file until we have enough to produce a merged file
        # Only used when multiple input files are merged in a single output (n-1) to pool input files together
        self.output_merge_queue: Dict[str, List[Tuple[str, EventRange]]] = dict()
        # Keep tracks of merge job definition that have been distributed to the driver for which we expect an update
        self.ditributed_merge_tasks: Dict[str, List[Tuple[str, EventRange]]] = dict()
        self.failed_count_by_file: Dict[str, int] = dict()
        self.last_status_print = time.time()
        self.taskstatus: Dict[str, TaskStatus] = dict()
        self.stop_saver = threading.Event()
        self.stop_cleaner = threading.Event()
        self.save_state_thread = ExThread(target=self._saver_thead_run, name="status-saver-thread")
        self.cleaner_thread = ExThread(target=self._cleaner_thead_run, name="cleaner-thread")

    def _cleaner_thead_run(self):
        """
        Thread that cleans the internal dictionnary of the bookkeeper
        """
        removed = []
        while not self.stop_cleaner.is_set():
            if os.path.isdir(self.output_dir):
                files = set(os.listdir(self.output_dir))
                # self._logger.debug(f"files in task dir: {files}")
                removed.clear()
                for task_status in self.taskstatus.values():
                    for merged_file in task_status._status[TaskStatus.MERGED].keys():
                        if self.stop_cleaner.is_set():
                            break
                        for temp_file in files:
                            if self.stop_cleaner.is_set():
                                break
                            if merged_file in temp_file:
                                os.remove(os.path.join(self.output_dir, temp_file))
                                removed.append(temp_file)
                        for temp_file in removed:
                            files.remove(temp_file)
            else:
                self._logger.debug(f"Dir {self.output_dir} doesn't exist")
            self.stop_cleaner.wait(60)

    def _saver_thead_run(self):

        while not self.stop_saver.is_set():
            self.save_status()
            # wait for 60s before next update or until the stop condition is met
            self.stop_saver.wait(60.0)

        # Perform a last drain of pending update before stopping
        self.save_status()

    def save_status(self):
        for task_status in self.taskstatus.values():
            task_status.save_status()

    def check_mergeable_files(self):
        """
        Goes through the current task status, checks if a file has been entierly processed (event ranges all simulated or failed) and
        if so adds the file to self.files_ready_to_merge
        """
        if self._hits_per_file >= self._events_per_file:
            self._check_mergeable_files_n_1()
        else:
            self._check_mergeable_files_1_n()

    def _check_mergeable_files_1_n(self):
        for input_file, event_ranges in self.ranges_to_merge.items():
            while len(event_ranges) >= self._hits_per_file:
                ranges_to_merge = event_ranges[-self._hits_per_file:]
                del event_ranges[-self._hits_per_file:]
                output_file = self._input_output_mapping[input_file].pop()
                self.files_ready_to_merge[output_file] = ranges_to_merge

    def _check_mergeable_files_n_1(self):
        for input_file, event_ranges in self.ranges_to_merge.items():
            # input file has been entierly processed
            if len(event_ranges) == self._events_per_file:
                # N-1 / 1-1 --> each input file has a predefined single output file name
                output_filename = self._input_output_mapping[input_file][0]
                if output_filename not in self.output_merge_queue:
                    self.output_merge_queue[output_filename] = []
                self.output_merge_queue[output_filename].extend(event_ranges)
                event_ranges.clear()
                if len(self.output_merge_queue[output_filename]) == self._hits_per_file:
                    self.files_ready_to_merge[output_filename] = self.output_merge_queue[output_filename]
                    del self.output_merge_queue[output_filename]

    def stop_saver_thread(self):
        if self.save_state_thread.is_alive():
            self.stop_saver.set()
            self.save_state_thread.join_with_ex()
            self.save_state_thread = ExThread(target=self._saver_thead_run, name="status-saver-thread")

    def stop_cleaner_thread(self):
        if self.cleaner_thread.is_alive():
            self.stop_cleaner.set()
            self.cleaner_thread.join_with_ex()
            self.cleaner_thread = ExThread(target=self._cleaner_thead_run, name="cleaner-thread")

    def start_threads(self):
        """
        Start the thread responsible for writing task status to disk
        """
        self.stop_saver.clear()
        self.stop_cleaner.clear()
        if not self.save_state_thread.is_alive():
            self.save_state_thread.start()
        if not self.cleaner_thread.is_alive():
            self.cleaner_thread.start()

    def add_jobs(self, jobs: Mapping[str, JobDef], start_threads=True) -> None:
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
        assert len(jobs) == 1
        self.jobs.add_jobs(jobs)
        for pandaID in self.jobs:
            job = self.jobs[pandaID]
            if job["taskID"] not in self.taskstatus:
                ts = TaskStatus(job, self.config)
                self.taskstatus[job['taskID']] = ts
                # TODO: have esdriver provide outputdir to make sure both are consistent
                self.output_dir = os.path.join(os.path.expandvars(self.config.ray.get("taskprogressbasedir")), str(job['taskID']))
                self._generate_input_output_mapping(job)
                self._generate_event_ranges(job, ts)
        if start_threads:
            self.start_threads()

    def _generate_input_output_mapping(self, job: PandaJob):
        """
        Goes through the list of input and ouput file names and matches expected output files for a given input file
        """
        # Filter out potential log files, only interested in HITS files
        output_files = [e for e in job["outFiles"].split(',') if e.startswith("HITS")]
        input_files = job["inFiles"].split(',')
        events_per_file = int(job['nEventsPerInputFile'])
        hits_per_file = int(job['esmergeSpec']['nEventsPerOutputFile'])

        input_output_mapping = dict()
        output_input_mapping = dict()
        # N-1 / 1-1 mapping
        if hits_per_file >= events_per_file:
            assert hits_per_file % events_per_file == 0
            n = hits_per_file // events_per_file
            assert len(input_files) == len(output_files) * n
            for i in range(len(input_files)):
                output_file = output_files[i // n]
                input_output_mapping[input_files[i]] = [output_file]
                if output_file not in output_input_mapping:
                    output_input_mapping[output_file] = []
                output_input_mapping[output_file].append(input_files[i])
        # 1-N mapping
        else:
            assert events_per_file % hits_per_file == 0
            n = events_per_file // hits_per_file
            assert len(input_files) * n == len(output_files)
            for i, j in zip(range(len(input_files)), range(0, len(output_files), n)):
                input_output_mapping[input_files[i]] = output_files[j:(j + n)]
                for output_file in output_files[j:(j + n)]:
                    output_input_mapping[output_file] = [input_files[i]]
        self._input_output_mapping = input_output_mapping
        self._output_input_mapping = output_input_mapping

    @staticmethod
    def generate_event_range_id(file: str, n: str):
        return f"{file}-{n}"

    def _generate_event_ranges(self, job: PandaJob, task_status: TaskStatus):
        """
        Generates all the event ranges which still need to be simulated and adds them to the
        EventRangeQueue of the job.

        Args:
            job: the job to which the generated event ranges will be assigned
            task_status: current status of the panda task
        """
        self._events_per_file = int(job['nEventsPerInputFile'])
        # We only ever get one job
        self._hits_per_file = int(job['esmergeSpec']['nEventsPerOutputFile'])
        is_n_to_one = self._hits_per_file >= self._events_per_file
        input_evnt_files = re.findall(r"\-\-inputEVNTFile=([\w\.\,]*) \-", job["jobPars"])
        if input_evnt_files:
            guids = job["GUID"].split(',')
            files = input_evnt_files[0].split(',')
            if "scopeIn" in job:
                scope = job["scopeIn"]
            else:
                scope = ""
            event_ranges = []
            merged_files = task_status._status[TaskStatus.MERGED]
            merging_files = task_status._status[TaskStatus.MERGING]
            simulated_ranges = task_status._status[TaskStatus.SIMULATED]
            failed_ranges = task_status._status[TaskStatus.FAILED]
            skip_event = False
            for file, guid in zip(files, guids):
                # if all the event ranges in the input file have been merge, continue to the next
                if file in merged_files:
                    continue
                file_simulated_ranges = simulated_ranges.get(file)
                file_failed_ranges = failed_ranges.get(file)
                # in n_to_one case, ranges from one input file all go into the same output file
                # so if we have a single failed range from that file, the entire file can be flagged as failed
                # in 1_to_n case, the input file is split in many output files so we can still partially process it.
                is_file_failed = file in failed_ranges and is_n_to_one
                file_merging_ranges = merging_files.get(file)
                for i in range(1, self._events_per_file + 1):
                    range_id = BookKeeper.generate_event_range_id(file, i)
                    event_range = EventRange(range_id, i, i, file, guid, scope)
                    # the file is marked as failed; all event ranges in the file are failed
                    if is_file_failed:
                        self.add_failed_range(event_range, file)
                        if file_failed_ranges is not None and range_id not in file_failed_ranges:
                            task_status.set_eventrange_failed(event_range)
                        continue
                    # checks if the event rang has already been merged in one of the output file
                    if file_merging_ranges:
                        for ranges in file_merging_ranges.values():
                            if range_id in ranges:
                                skip_event = True
                                break
                        if skip_event:
                            skip_event = False
                            continue
                    # event range hasn't been merged but already simulated, add it as ready to be merged
                    if file_simulated_ranges is not None and range_id in file_simulated_ranges:
                        item = (file_simulated_ranges[range_id]["path"], event_range)
                        if event_range.PFN not in self.ranges_to_merge:
                            self.ranges_to_merge[event_range.PFN] = [item]
                        else:
                            self.ranges_to_merge[event_range.PFN].append(item)
                    # only for 1-to-n jobs
                    elif file_failed_ranges is not None and range_id in file_failed_ranges:
                        self.add_failed_range(event_range, file)
                    # event range hasn't been simulated, add it to the event range queue
                    else:
                        event_ranges.append(event_range)
            self._logger.debug(f"Generated {len(event_ranges)} event ranges")
            job.event_ranges_queue.add_new_event_ranges(event_ranges)

    def add_failed_range(self, evnt_range: EventRange, file):

        input_file_for_range = os.path.basename(evnt_range.PFN)
        output_file_for_range = self._input_output_mapping[input_file_for_range]

        # case N-1 / 1-1 : we need to invalidate all the event ranges that should have been merged together
        if len(output_file_for_range) == 1:
            # retrieve other input files that should be merged together
            input_files = self._output_input_mapping[output_file_for_range[0]]
            assert input_file_for_range in input_files
            job: PandaJob = self.jobs[list(iter(self.jobs))[0]]
            for file in input_files:
                for i in range(self._events_per_file):
                    range_id = BookKeeper.generate_event_range_id(file, i)
                    job.event_ranges_queue.update_range_state(range_id, EventRange.FAILED)

        # case 1-N : we don't need to do anything as we can still potentially merge part of the input file
        else:
            pass
        if file not in self.failed_count_by_file:
            self.failed_count_by_file[file] = 1
        else:
            self.failed_count_by_file[file] += 1

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

    def get_file_to_merge(self) -> Optional[Tuple[str, List[Tuple[str, EventRange]]]]:
        """
        Returns a merge tasks available for an arbitrary input file if available, None otherwise.
        """
        if self.files_ready_to_merge:
            merge_task = self.files_ready_to_merge.popitem()
            self.ditributed_merge_tasks[merge_task[0]] = merge_task[1]
            return merge_task
        return None

    def report_merged_file(self, taskID: str, merged_output_file: str, merged_event_ranges: Mapping[str, Mapping[str, str]]):
        assert merged_output_file in self.ditributed_merge_tasks
        del self.ditributed_merge_tasks[merged_output_file]
        self.taskstatus[taskID].set_file_merged(self._output_input_mapping[merged_output_file], merged_output_file, merged_event_ranges)

    def report_failed_merge_transform(self, taskID: str, merged_output_file: str):
        assert merged_output_file in self.ditributed_merge_tasks
        old_task = self.ditributed_merge_tasks.pop(merged_output_file)
        self.files_ready_to_merge[merged_output_file] = old_task

    def process_event_ranges_update(self, actor_id: str, event_ranges_update: Union[Sequence[PilotEventRangeUpdateDef], EventRangeUpdate]):
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
        for r in event_ranges_update[panda_id]:
            if 'eventRangeID' in r and r['eventRangeID'] in actor_ranges:
                range_id = r['eventRangeID']
                actor_ranges.remove(range_id)
                evnt_range = job_ranges[range_id]
                if r['eventStatus'] == EventRange.DONE:
                    task_status.set_eventrange_simulated(evnt_range, r['path'])
                    if evnt_range.PFN not in self.ranges_to_merge:
                        self.ranges_to_merge[evnt_range.PFN] = list()
                    self.ranges_to_merge[evnt_range.PFN].append((r["path"], evnt_range))
                elif r['eventStatus'] in [EventRange.FAILED, EventRange.FATAL]:
                    self._logger.info(f"Received failed event from {actor_id}: {r}")
                    task_status.set_eventrange_failed(job_ranges[range_id])
                    self.add_failed_range(evnt_range, evnt_range.PFN)
        now = time.time()
        if now - self.last_status_print > 60:
            self.last_status_print = now
            self.print_status()

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
