import os
import time
from queue import Queue, Empty
from typing import List, Dict, Union, Any

import ray

from raythena.actors.esworker import ESWorker
from raythena.actors.loggingActor import LoggingActor
from raythena.drivers.baseDriver import BaseDriver
from raythena.drivers.communicators.baseCommunicator import BaseCommunicator
from raythena.utils.config import Config
from raythena.utils.eventservice import (EventRangeRequest, PandaJobRequest,
                                         EventRangeUpdate, Messages, PandaJobQueue,
                                         EventRange, PandaJob, JobReport)
from raythena.utils.plugins import PluginsRegistry
from raythena.utils.ray import (build_nodes_resource_list, get_node_ip)
from raythena.utils.timing import CPUMonitor

EventRangeTypeHint = Dict[str, str]
PandaJobTypeHint = Dict[str, str]


class BookKeeper(object):
    """
    Performs bookkeeping of jobs and event ranges distributed to workers
    """

    def __init__(self, logging_actor: LoggingActor, config: Config) -> None:
        self.jobs = PandaJobQueue()
        self.logging_actor = logging_actor
        self.config = config
        self.actors: Dict[str, Union[str, None]] = dict()
        self.rangesID_by_actor: Dict[str, List[str]] = dict()
        self.finished_range_by_input_file: Dict[str, List[Dict]] = dict()
        self.ranges_to_tar_by_input_file: Dict[str, List[Dict]] = dict()
        self.start_time = time.time()
        self.finished_by_time = []
        self.finished_by_time.append((time.time(), 0))
        self.monitortime = self.config.ray['monitortime']
        self.logging_actor.debug.remote("BookKeeper", f"Num_finished: start_time {self.start_time}", time.asctime())

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

    def add_finished_event_ranges(self) -> None:
        """
        Add Number of finished event ranges to finished_by_time list.
        Each entry is the list (time stamp (time.time()), job_ranges.nranges_done()

        Args:
            None

        Returns:
            None
        """
        nfinished = 0
        for pandaID in self.jobs:
            job_ranges = self.jobs.get_event_ranges(pandaID)
            nfinished = nfinished + job_ranges.nranges_done()
        # get the previous time stamp
        time_tuple = self.finished_by_time[-1]
        time_stamp = time_tuple[0]
        now = time.time()
        delta_time = now - time_stamp
        # Record number of finished jobs at allowed time interval
        if int(delta_time) >= self.monitortime:
            time_tuple = (now, nfinished)
            self.finished_by_time.append(time_tuple)
            self.logging_actor.debug.remote("BookKeeper",
                                            f"add to finished_by_time {len(self.finished_by_time)} time_tuple:  {time_tuple} ",
                                            time.asctime())

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
        job_id, _ = self.jobs.next_job_id_to_process()
        return job_id is not None

    def assign_job_to_actor(self, actor_id: str) -> Union[PandaJob, None]:
        """
        Retrieve a job from the job queue to be assigned to a worker

        Args:
            actor_id: actor to which the job should be assigned to

        Returns:
            job worker_id of assigned job, None if no job is available
        """
        job_id, nranges = self.jobs.next_job_id_to_process()
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
            self.rangesID_by_actor[actor_id] = list()
        ranges = self.jobs.get_event_ranges(
            self.actors[actor_id]).get_next_ranges(n)
        for r in ranges:
            self.rangesID_by_actor[actor_id].append(r.eventRangeID)
        return ranges

    def process_event_ranges_update(
        self, actor_id: str, event_ranges_update: Union[dict, EventRangeUpdate]
    ) -> Union[EventRangeUpdate, None]:
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
        self.logging_actor.debug.remote(
            "BookKeeper", f"Built rangeUpdate: {event_ranges_update}", time.asctime())
        self.jobs.process_event_ranges_update(event_ranges_update)
        job_ranges = self.jobs.get_event_ranges(panda_id)

        for r in event_ranges_update[panda_id]:
            if 'eventRangeID' in r and r['eventRangeID'] in self.rangesID_by_actor[actor_id]:
                range_id = r['eventRangeID']
                if r['eventStatus'] == EventRange.DONE:
                    self.rangesID_by_actor[actor_id].remove(range_id)
                    event_range = job_ranges[range_id]
                    file_basename = os.path.basename(event_range.PFN)
                    if file_basename not in self.finished_range_by_input_file:
                        self.finished_range_by_input_file[file_basename] = list()
                    if file_basename not in self.ranges_to_tar_by_input_file:
                        self.ranges_to_tar_by_input_file[file_basename] = list()
                    self.finished_range_by_input_file[file_basename].append(r)
                    self.ranges_to_tar_by_input_file[file_basename].append(r)

        log_message = str()
        for input_file, ranges in self.ranges_to_tar_by_input_file.items():
            log_message = f"\n{input_file}: Type: {type (ranges)} values: {repr(ranges)}"
        self.logging_actor.debug.remote("BookKeeper", log_message, time.asctime())
        log_message = ""
        for input_file, ranges in self.finished_range_by_input_file.items():
            log_message = f"\n{input_file}: {len(ranges)} event ranges processed{log_message}"
        self.logging_actor.debug.remote("BookKeeper", log_message, time.asctime())
        self.logging_actor.info.remote("BookKeeper",
                                       (
                                           f"\nEvent ranges status for job { panda_id}:\n"
                                           f"  Ready: {job_ranges.nranges_available()}\n"
                                           f"  Assigned: {job_ranges.nranges_assigned()}\n"
                                           f"  Failed: {job_ranges.nranges_failed()}\n"
                                           f"  Finished: {job_ranges.nranges_done()}\n"
                                       ), time.asctime())

        now = time.time()
        self.logging_actor.debug.remote("BookKeeper", f"Num_finished: {job_ranges.nranges_done()} {now} {len(self.finished_by_time)}", time.asctime())

        return event_ranges_update

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
        self.logging_actor.warn.remote(
            "BookKeeper",
            f"{actor_id} finished with {len(actor_ranges)} remaining to process", time.asctime()
        )
        for rangeID in actor_ranges:
            self.logging_actor.warn.remote(
                "BookKeeper",
                f"{actor_id} finished without processing range {rangeID}", time.asctime())
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

    def __init__(self, config: Config) -> None:
        """
        Initialize ray custom resources, logging actor, a bookKeeper instance and other attributes.
        The harvester communicator is also initialized and an initial JobRequest is sent

        Args:
            config: application config
        """
        super().__init__(config)
        self.id = f"Driver_node:{get_node_ip()}"
        self.logging_actor: LoggingActor = LoggingActor.remote(self.config)

        self.nodes = build_nodes_resource_list(self.config, run_actor_on_head=False)

        self.requests_queue = Queue()
        self.jobs_queue = Queue()
        self.event_ranges_queue = Queue()

        self.logging_actor.debug.remote(self.id,
                                        f"Driver initialized, running Ray {ray.__version__}", time.asctime())

        workdir = os.path.expandvars(self.config.ray.get('workdir'))
        if not workdir or not os.path.exists(workdir):
            self.logging_actor.warn.remote(
                self.id,
                f"ray workdir '{workdir}' doesn't exist... using cwd {os.getcwd()}", time.asctime()
            )
            workdir = os.getcwd()
        self.config.ray['workdir'] = workdir

        self.cpu_monitor = CPUMonitor(os.path.join(workdir, "cpu_monitor_driver.json"))
        self.cpu_monitor.start()

        registry = PluginsRegistry()
        self.communicator_class = registry.get_plugin(self.config.harvester['communicator'])

        self.communicator: BaseCommunicator = self.communicator_class(self.requests_queue,
                                                                      self.jobs_queue,
                                                                      self.event_ranges_queue,
                                                                      config)
        self.communicator.start()
        self.requests_queue.put(PandaJobRequest())
        self.logging_actor.debug.remote(self.id,
                                        "Sent job request to harvester", time.asctime())
        self.actors = dict()
        self.actors_message_queue = list()
        self.bookKeeper = BookKeeper(self.logging_actor, config)
        self.terminated = list()
        self.running = True
        self.n_eventsrequest = 0
        self.first_event_range_request = True
        self.no_more_events = False
        self.timeoutinterval = self.config.ray['timeoutinterval']
        self.tarinterval = self.config.ray['tarinterval']
        self.tarmaxfilesize = self.config.ray['tarmaxfilesize']
        self.tarmaxprocesses = self.config.ray['tarmaxprocesses']
        self.tar_timestamp = time.time()

        
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
        for actor in self.actors.values():
            self.actors_message_queue.append(actor.get_message.remote())

    def create_actors(self) -> None:
        """
        Create actors by using custom resources available in self.nodes

        Returns:
            None
        """

        for i, node in enumerate(self.nodes):
            nodeip = node['NodeManagerAddress']
            node_constraint = f"node:{nodeip}"
            actor_id = f"Actor_{i}"
            actor_args = {
                'actor_id': actor_id,
                'config': self.config,
                'logging_actor': self.logging_actor
            }
            actor = ESWorker.options(resources={
                node_constraint: 1
            }).remote(**actor_args)
            self.actors[actor_id] = actor
            self.logging_actor.debug.remote(
                self.id, f"Created actor {actor_id} with ip address {nodeip}", time.asctime())

    def handle_actors(self) -> None:
        """
        Main function handling messages from all ray actors.

        Returns:
            None
        """
        new_messages, self.actors_message_queue = ray.wait(
            self.actors_message_queue, num_returns=1)
        total_sent = 0
        while new_messages and self.running:
            self.logging_actor.debug.remote(
                self.id, f"Start handling messages batch of {len(new_messages)} actors", time.asctime())
            for ray_message_id in new_messages:
                try:
                    actor_id, message, data = ray.get(ray_message_id)
                except Exception as e:
                    self.handle_actor_exception(e)
                else:
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
            self.logging_actor.debug.remote(
                self.id, "Finished handling messages batch", time.asctime())
            self.on_tick()
            # check if finished any events, set timeout interval accordingly
            if self.bookKeeper.have_finished_events():
                timeoutinterval = self.timeoutinterval
            else:
                timeoutinterval = None
            self.logging_actor.debug.remote(
                self.id, f"set timeout interval on waiting on new messages ({format(timeoutinterval)})", time.asctime())
            self.logging_actor.debug.remote(
                self.id, "Waiting on new messages from actors", time.asctime())
            new_messages, self.actors_message_queue = ray.wait(
                self.actors_message_queue, timeout=timeoutinterval)

        self.logging_actor.debug.remote(
            self.id, "Finished handling the Actors. Raythena will shutdown now.", time.asctime())

    def handle_actor_done(self, actor_id: str) -> bool:
        """
        Handle worker that finished processing a job

        Args:
            actor_id: actor which finished processing a job

        Returns:
            True if more jobs should be sent to the actor
        """
        # TODO actor should drain job update, range update queue and send a final update.
        # try to assign a new job to the actor
        self.logging_actor.info.remote(
            self.id,
            f"{actor_id} finished processing current job, checking for a new job...",
            time.asctime()
        )
        has_jobs = self.bookKeeper.has_jobs_ready()
        if has_jobs:
            self.logging_actor.info.remote(
                self.id, f"More jobs to be processed by {actor_id}", time.asctime())
            self.actors_message_queue.append(self[actor_id].mark_new_job.remote())
        else:
            self.logging_actor.info.remote(
                self.id, f" no more job for {actor_id}", time.asctime())
            self.terminated.append(actor_id)
            self.bookKeeper.process_actor_end(actor_id)
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
        self.logging_actor.info.remote(
            self.id, f"{actor_id} sent a eventranges update", time.asctime())
        eventranges_update = self.bookKeeper.process_event_ranges_update(
            actor_id, data)
        self.requests_queue.put(eventranges_update)
        self.actors_message_queue.append(
            self[actor_id].get_message.remote())

    def handle_update_job(self, actor_id: str, data: Any) -> None:
        """
        Handle worker job update

        Args:
            actor_id: worker sending the job update
            data: job update

        Returns:
            None
        """
        self.logging_actor.info.remote(
            self.id, f"{actor_id} sent a job update", time.asctime())
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
        panda_id = self.bookKeeper.actors[actor_id]
        n_ranges = data[panda_id]['nRanges']
        evt_range = self.bookKeeper.fetch_event_ranges(
            actor_id, n_ranges)
        # did not fetch enough event and harvester might have more, needs to get more events now
        while (len(evt_range) < n_ranges and
               not self.bookKeeper.is_flagged_no_more_events(
                   panda_id)):
            self.logging_actor.debug.remote(
                self.id,
                f"Not enough event ranges. available: {len(evt_range)} requested: {n_ranges}", time.asctime()
            )
            self.request_event_ranges(block=True)
            evt_range += self.bookKeeper.fetch_event_ranges(
                actor_id, n_ranges - len(evt_range))
        if evt_range:
            total_sent += len(evt_range)
            self.logging_actor.info.remote(
                self.id,
                f"sending {len(evt_range)} ranges to {actor_id}. Total sent: {total_sent}", time.asctime()
            )
        else:
            self.logging_actor.info.remote(
                self.id, f"No more ranges to send to {actor_id}", time.asctime())
        self.actors_message_queue.append(self[actor_id].receive_event_ranges.remote(
            Messages.REPLY_OK if evt_range else
            Messages.REPLY_NO_MORE_EVENT_RANGES, evt_range))
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

        if job:
            self.logging_actor.info.remote(
                self.id, f"Sending job {job.get_id()} to {actor_id}", time.asctime())
        else:
            self.logging_actor.info.remote(self.id, f"No jobs available for {actor_id}", time.asctime())
        self.actors_message_queue.append(self[actor_id].receive_job.remote(
            Messages.REPLY_OK
            if job else Messages.REPLY_NO_MORE_JOBS, job))

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
                    self.logging_actor.debug.remote(
                        self.id,
                        f"Job {pandaID} has no more events. Skipping request...", time.asctime()
                    )
                    continue
                n_available_ranges = self.bookKeeper.n_ready(pandaID)
                job = self.bookKeeper.jobs[pandaID]
                # each pilot will request for 'coreCount * 2' event ranges
                # and we use an additional safety factor of 2
                n_events = int(job['coreCount']) * len(self.nodes) * 2 * 2
                self.logging_actor.debug.remote(
                    self.id, f"Calculate num event ranges - {n_events} = {int(job['coreCount'])} * {len(self.nodes)} * 2 * 2 ", time.asctime())
                if n_available_ranges < n_events:
                    event_request.add_event_request(pandaID,
                                                    n_events,
                                                    job['taskID'],
                                                    job['jobsetID'])

            if len(event_request) > 0:
                self.logging_actor.debug.remote(
                    self.id, f"Sending event ranges request to harvester for {n_events} events", time.asctime())
                self.requests_queue.put(event_request)
                self.n_eventsrequest += 1

        if self.n_eventsrequest > 0:
            try:
                ranges = self.event_ranges_queue.get(block)
                self.logging_actor.debug.remote(self.id,
                                                "received reply from harvester", time.asctime())
                n_received_events = 0
                for pandaID, ranges_list in ranges.items():
                    n_received_events += len(ranges_list)
                    self.logging_actor.debug.remote(self.id, f"got ranges for pandaID {pandaID}: {len(ranges_list)}", time.asctime())
                if self.first_event_range_request:
                    self.first_event_range_request = False
                    if (n_received_events < int(job['coreCount']) * len(self.nodes)):
                        self.logging_actor.error.remote(self.id, "Got too few events initially. Exiting...", time.asctime())
                        self.stop()
                self.bookKeeper.add_event_ranges(ranges)
                self.n_eventsrequest -= 1
            except Empty:
                pass

    def on_tick(self) -> None:
        """
        Performs actions that should be executed regularly, after handling a batch of actor messages.

        Returns:
            None
        """
        if self.no_more_events:
            self.logging_actor.info.remote(self.id, "no more events available and some workers are already idle. Shutting down...", time.asctime())
            self.stop()
        self.bookKeeper.add_finished_event_ranges()
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
        self.logging_actor.info.remote(self.id, f"Started driver {self}", time.asctime())
        # gets initial jobs and send an eventranges request for each jobs
        jobs = self.jobs_queue.get()
        if not jobs:
            self.logging_actor.critical.remote(
                self.id, "No jobs provided by communicator, stopping...", time.asctime())
            return
        self.logging_actor.debug.remote(self.id,
                                        f"Received reply to the job request:\n{jobs}", time.asctime())
        self.bookKeeper.add_jobs(jobs)

        # sends an initial event range request
        self.request_event_ranges(block=True)
        if not self.bookKeeper.has_jobs_ready():
            self.cpu_monitor.stop()
            self.communicator.stop()
            self.logging_actor.critical.remote(
                self.id, "Couldn't fetch a job with event ranges, stopping...", time.asctime())
            time.sleep(5)
            return

        for pandaID in self.bookKeeper.jobs:
            cjob = self.bookKeeper.jobs[pandaID]
            os.makedirs(
                os.path.join(self.config.ray['workdir'], cjob['PandaID']))

        self.create_actors()

        self.start_actors()

        try:
            self.handle_actors()
        except Exception as e:
            self.logging_actor.error.remote(
                self.id, f"Error while handling actors: {e}. stopping...", time.asctime())

        self.requests_queue.put(JobReport())

        self.communicator.stop()
        self.cpu_monitor.stop()

        self.logging_actor.debug.remote(
            self.id, "Communicator and cpu_monitor stopped.", time.asctime())

        time.sleep(5)

        self.logging_actor.debug.remote(
            self.id, "Exitting the Driver...", time.asctime())

    def stop(self) -> None:
        """
        Stop the driver.

        Returns:
            None
        """
        self.logging_actor.info.remote(self.id, "Graceful shutdown...", time.asctime())
        self.running = False
        self.cleanup()

    def handle_actor_exception(self, ex: Exception) -> None:
        """
        Handle exception that occurred in an actor process

        Args:
            ex: exception raised in actor process

        Returns:
            None
        """
        self.logging_actor.info.remote(self.id, f"Caught exception in actor {ex}", time.asctime())
        pass
