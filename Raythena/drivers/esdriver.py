import os
import time
from queue import Queue, Empty
from typing import List, Dict, Union

import ray

from Raythena.actors.esworker import ESWorker
from Raythena.actors.loggingActor import LoggingActor
from Raythena.drivers.baseDriver import BaseDriver
from Raythena.utils.config import Config
from Raythena.utils.eventservice import (EventRangeRequest, PandaJobRequest,
                                         EventRangeUpdate, Messages,
                                         PandaJobQueue, EventRange)
from Raythena.utils.importUtils import import_from_string
from Raythena.utils.ray import (build_nodes_resource_list, get_node_ip)

EventRangeTypeHint = Dict[str, str]
PandaJobTypeHint = Dict[str, str]


class BookKeeper(object):

    def __init__(self, logging_actor: LoggingActor, config: Config) -> None:
        self.jobs = PandaJobQueue()
        self.logging_actor = logging_actor
        self.config = config
        self.actors = dict()
        self.rangesID_by_actor = dict()

    def add_jobs(self, jobs: Dict[str, PandaJobTypeHint]) -> None:
        self.jobs.add_jobs(jobs)

    def add_event_ranges(
            self, event_ranges: Dict[str, List[EventRangeTypeHint]]) -> None:
        self.jobs.process_event_ranges_reply(event_ranges)

    def has_jobs_ready(self) -> bool:
        job_id, _ = self.jobs.next_job_id_to_process()
        return job_id is not None

    def assign_job_to_actor(self, actor_id: str) -> str:
        job_id, nranges = self.jobs.next_job_id_to_process()
        self.actors[actor_id] = job_id
        return self.jobs[job_id] if job_id else None

    def fetch_event_ranges(self, actor_id: str, n: int) -> List[EventRange]:
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
        self, actor_id: str, event_ranges_update: EventRangeUpdate
    ) -> Union[EventRangeUpdate, None]:
        panda_id = self.actors.get(actor_id, None)
        if not panda_id:
            return

        if not isinstance(event_ranges_update, EventRangeUpdate):
            event_ranges_update = EventRangeUpdate.build_from_dict(
                panda_id, event_ranges_update)
        self.logging_actor.info.remote(
            "BookKeeper", f"Built rangeUpdate: {event_ranges_update}")
        self.jobs.process_event_ranges_update(event_ranges_update)
        for r in event_ranges_update[panda_id]:
            if r['eventRangeID'] in self.rangesID_by_actor[
                    actor_id] and r['eventStatus'] != "running":
                self.rangesID_by_actor[actor_id].remove(r['eventRangeID'])
        return event_ranges_update

    def process_actor_end(self, actor_id: str) -> None:
        panda_id = self.actors.get(actor_id, None)
        if not panda_id:
            return
        actor_ranges = self.rangesID_by_actor.get(actor_id, None)
        if not actor_ranges:
            return
        self.logging_actor.warn.remote(
            "BookKeeper",
            f"{actor_id} finished with {len(actor_ranges)} remaining to process"
        )
        for rangeID in actor_ranges:
            self.logging_actor.warn.remote(
                "BookKeeper",
                f"{actor_id} finished without processing range {rangeID}")
            self.jobs.get_event_ranges(panda_id).update_range_state(
                rangeID, EventRange.READY)
        actor_ranges.clear()
        self.actors[actor_id] = None

    def n_ready(self, panda_id: str) -> int:
        return self.jobs.get_event_ranges(panda_id).nranges_available()

    def is_flagged_no_more_events(self, panda_id: str) -> bool:
        return self.jobs.get_event_ranges(panda_id).no_more_ranges


class ESDriver(BaseDriver):

    def __init__(self, config: Config) -> None:
        super().__init__(config)
        self.id = f"Driver_node:{get_node_ip()}"
        self.logging_actor = LoggingActor.remote(self.config)

        self.nodes = build_nodes_resource_list(self.config)

        # As actors request 2 * corepernode, make sure to request enough events to serve all actors
        self.n_events_per_request = self.config.resources['corepernode'] * len(
            self.nodes) * 2

        self.requests_queue = Queue()
        self.jobs_queue = Queue()
        self.event_ranges_queue = Queue()

        workdir = os.path.expandvars(self.config.ray.get('workdir'))
        if not workdir or not os.path.exists(workdir):
            self.logging_actor.warn.remote(
                self.id,
                f"ray workdir '{workdir}' doesn't exist... using cwd {os.getcwd()}"
            )
            workdir = os.getcwd()
        self.config.ray['workdir'] = workdir

        self.communicator_class = import_from_string(
            f"Raythena.drivers.communicators.{self.config.harvester['communicator']}"
        )
        self.communicator = self.communicator_class(self.requests_queue,
                                                    self.jobs_queue,
                                                    self.event_ranges_queue,
                                                    config)
        self.communicator.start()
        self.requests_queue.put(PandaJobRequest())
        self.actors = dict()
        self.actors_message_queue = list()
        self.bookKeeper = BookKeeper(self.logging_actor, config)
        self.terminated = list()
        self.running = True
        self.n_eventsrequest = 0

    def __str__(self):
        return self.__dict__.__str__()

    def __getitem__(self, key):
        return self.actors[key]

    def start_actors(self) -> None:
        """
        Initialize actor communication
        """
        for actor in self.actors.values():
            self.actors_message_queue.append(actor.get_message.remote())

    def create_actors(self) -> None:
        """
        Create new ray actors, one per node
        """

        for node_constraint in self.nodes:
            _, _, nodeip = node_constraint.partition('@')
            actor_id = f"Actor_{nodeip}"
            actor_args = {
                'actor_id': actor_id,
                'config': self.config,
                'logging_actor': self.logging_actor
            }
            actor = ESWorker.options(resources={
                node_constraint: 1
            }).remote(**actor_args)
            self.actors[actor_id] = actor

    def handle_actors(self) -> None:

        new_messages, self.actors_message_queue = ray.wait(
            self.actors_message_queue)
        total_sent = 0
        while new_messages and self.running:
            for actor_id, message, data in ray.get(new_messages):
                if message == Messages.IDLE:
                    pass
                if message == Messages.REQUEST_NEW_JOB:
                    # important to have at least one job with event ranges assigned as job without eventranges will not get assigned
                    job = self.bookKeeper.assign_job_to_actor(actor_id)
                    self.logging_actor.info.remote(
                        self.id, f"Sending {job} to {actor_id}")
                    self[actor_id].receive_job.remote(
                        Messages.REPLY_OK
                        if job else Messages.REPLY_NO_MORE_JOBS, job)
                elif message == Messages.REQUEST_EVENT_RANGES:

                    pandaID = self.bookKeeper.actors[actor_id]
                    nranges = data[pandaID]['nRanges']
                    evt_range = self.bookKeeper.fetch_event_ranges(
                        actor_id, nranges)

                    # did not fetch enough event and harvester might have more, needs to get more events now
                    while len(
                            evt_range
                    ) < nranges and not self.bookKeeper.is_flagged_no_more_events(
                            pandaID):
                        self.logging_actor.debug.remote(
                            self.id,
                            f"Not enough event ranges to satisfy actor request. available: {len(evt_range)} requested: {nranges}"
                        )
                        self.request_event_ranges(block=True)
                        evt_range += self.bookKeeper.fetch_event_ranges(
                            actor_id, nranges - len(evt_range))

                    if evt_range:
                        total_sent += len(evt_range)
                        self.logging_actor.info.remote(
                            self.id,
                            f"sending {len(evt_range)} ranges to {actor_id}. Total sent: {total_sent}"
                        )
                    else:
                        self.logging_actor.info.remote(
                            self.id, f"No more ranges to send to {actor_id}")
                    self[actor_id].receive_event_ranges.remote(
                        Messages.REPLY_OK if evt_range else
                        Messages.REPLY_NO_MORE_EVENT_RANGES, evt_range)
                elif message == Messages.UPDATE_JOB:
                    self.logging_actor.info.remote(
                        self.id, f"{actor_id} sent a job update: {data}")
                elif message == Messages.UPDATE_EVENT_RANGES:
                    self.logging_actor.info.remote(
                        self.id, f"{actor_id} sent a eventranges update")
                    eventranges_update = self.bookKeeper.process_event_ranges_update(
                        actor_id, data)
                    self.requests_queue.put(eventranges_update)
                elif message == Messages.PROCESS_DONE:  # TODO actor should drain jobupdate, rangeupdate queue and send a final update.
                    # try to assign a new job to the actor
                    self.logging_actor.info.remote(
                        self.id,
                        f"{actor_id} finished processing current job, checking for a new job..."
                    )
                    has_jobs = self.bookKeeper.has_jobs_ready()
                    if has_jobs:
                        self.logging_actor.info.remote(
                            self.id, f"More jobs to be processed by {actor_id}")
                        self[actor_id].mark_new_job.remote()
                    else:
                        self.logging_actor.info.remote(
                            self.id, f" no more job for {actor_id}")
                        self.terminated.append(actor_id)
                        self.bookKeeper.process_actor_end(actor_id)
                        # do not get new messages from this actor
                        continue

                self.actors_message_queue.append(
                    self[actor_id].get_message.remote())
            self.on_tick()
            new_messages, self.actors_message_queue = ray.wait(
                self.actors_message_queue)

    def request_event_ranges(self, block: bool = False) -> None:
        """
        If no event range request, checks if any jobs needs more ranges, and if so, sends a requests to harvester.
        If an event range request has been sent (including one from the same call of this function),
        checks if a reply is available. If so, add the ranges to the bookKeeper.
        If block == true and a request has been sent, blocks until a reply is received
        @param block: wait on the response from harvester communicator if true,
        otherwise try to fetch response if available
        """

        if self.n_eventsrequest == 0:

            evnt_request = EventRangeRequest()
            for pandaID in self.bookKeeper.jobs:
                if self.bookKeeper.is_flagged_no_more_events(pandaID):
                    self.logging_actor.debug.remote(
                        self.id,
                        f"Job {pandaID} has no more events. Skipping request..."
                    )
                    continue
                n_available_ranges = self.bookKeeper.n_ready(pandaID)
                if n_available_ranges < self.config.resources['corepernode'] * 2:
                    job = self.bookKeeper.jobs[pandaID]
                    evnt_request.add_event_request(pandaID,
                                                   self.n_events_per_request,
                                                   job['taskID'],
                                                   job['jobsetID'])

            if len(evnt_request) > 0:
                self.logging_actor.debug.remote(
                    self.id, f"Sending request {evnt_request}")
                self.requests_queue.put(evnt_request)
                self.n_eventsrequest += 1

        if self.n_eventsrequest > 0:
            try:
                ranges = self.event_ranges_queue.get(block)
                self.logging_actor.debug.remote(self.id,
                                                f"Fetched eventrange response")
                self.bookKeeper.add_event_ranges(ranges)
                self.n_eventsrequest -= 1
            except Empty:
                pass

    def on_tick(self) -> None:
        self.request_event_ranges()

    def cleanup(self) -> None:
        handles = list()
        for name, handle in self.actors.items():
            if name not in self.terminated:
                handles.append(handle.interrupt.remote())
                self.terminated.append(name)
        ray.get(handles)

    def run(self) -> None:
        self.logging_actor.info.remote(self.id, f"Started driver {self}")
        # gets initial jobs and send an eventranges request for each jobs
        jobs = self.jobs_queue.get()
        if not jobs:
            self.logging_actor.critical.remote(
                self.id, "No jobs provided by communicator, stopping...")
            return

        self.bookKeeper.add_jobs(jobs)
        for pandaID in self.bookKeeper.jobs:
            cjob = self.bookKeeper.jobs[pandaID]
            os.makedirs(
                os.path.join(self.config.ray['workdir'], cjob['PandaID']))

        # sends an initial event range request
        self.request_event_ranges(block=True)
        if not self.bookKeeper.has_jobs_ready():
            self.communicator.stop()
            self.logging_actor.critical.remote(
                self.id, f"Couldn't fetch a job with event ranges, stopping...")
            time.sleep(5)
            return

        self.create_actors()

        self.start_actors()

        try:
            self.handle_actors()
        except Exception as e:
            self.logging_actor.error.remote(
                self.id, f"Error while handling actors: {e}. stopping...")

        self.communicator.stop()

        time.sleep(5)

    def stop(self) -> None:
        self.logging_actor.info.remote(self.id, "Graceful shutdown...")
        self.running = False
        self.cleanup()
