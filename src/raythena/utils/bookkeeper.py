from raythena.utils.config import Config
from raythena.utils.eventservice import PandaJobQueue, EventRange, PandaJob, EventRangeUpdate, EventRangeDef, JobDef, PilotEventRangeUpdateDef
from raythena.utils.logging import make_logger

from typing import Dict, Set, Optional, List, Mapping, Sequence, Union, Tuple

import time
import os


class BookKeeper(object):
    """
    Performs bookkeeping of jobs and event ranges distributed to workers
    """

    def __init__(self, config: Config) -> None:
        self.jobs: PandaJobQueue = PandaJobQueue()
        self.config: Config = config
        self._logger = make_logger(self.config, "BookKeeper")
        self.actors: Dict[str, Optional[str]] = dict()
        self.rangesID_by_actor: Dict[str, Set[str]] = dict()
        self.finished_range_by_input_file: Dict[str, List[EventRangeDef]] = dict()
        self.ranges_to_tar_by_input_file: Dict[str, List[EventRangeDef]] = dict()
        self.ranges_to_tar: List[List[EventRangeDef]] = list()
        self.ranges_tarred_up: List[List[EventRangeDef]] = list()
        self.ranges_tarred_by_output_file: Dict[str, List[EventRangeDef]] = dict()
        self.start_time: float = time.time()
        self.tarmaxfilesize: int = self.config.ray['tarmaxfilesize']
        self.last_status_print = time.time()

    def get_ranges_to_tar(self) -> List[List[EventRangeDef]]:
        """
        Return a list of lists of event Ranges to be written to tar files.
        Essentially the same structure as get_ranges_to_tar_by_input_file but without the input file name as key.

        Returns:
            List of Lists of Event Ranges to be put into tar files
        """
        return self.ranges_to_tar

    def get_ranges_to_tar_by_input_file(self) -> Dict[str, List[EventRangeDef]]:
        """
        Return the dictionary of event Ranges to be written to tar files organized by input file.

        Returns:
            dict of Event Ranges organized by input file
        """
        return self.ranges_to_tar_by_input_file

    def create_ranges_to_tar(self) -> bool:
        """
        using the event ranges organized by input file in ranges_to_tar_by_input_file
        loop over the entries creating a list of lists which contains all of event ranges to be tarred up.
        update the dictionary of event Ranges to be written to tar files organized by input files
        removing the event ranges event Range lists organized by input files

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

    def add_jobs(self, jobs: Mapping[str, JobDef]) -> None:
        """
        Register new jobs. Event service jobs will not be assigned to worker until event ranges are added to the job

        Args:
            jobs: job dict

        Returns:
            None
        """
        self.jobs.add_jobs(jobs)

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
