import os

import psutil
import ray
import json

from queue import Queue

from Raythena.actors.esworker import ESWorker
from Raythena.actors.loggingActor import LoggingActor
from Raythena.utils.eventservice import EventRangeRequest, PandaJobRequest, EventRangeUpdate, PandaJobUpdate, Messages, PandaJobQueue
from Raythena.utils.exception import BaseRaythenaException
from Raythena.utils.importUtils import import_from_string
from Raythena.utils.ray import (build_nodes_resource_list, cluster_size,
                                get_node_ip)

from .baseDriver import BaseDriver


class BookKeeper:

    def __init__(self, logging_actor, config):
        self.jobs = PandaJobQueue()
        self.actors = dict()
    
    def add_jobs(self, jobs):
        self.jobs.add_jobs(jobs)
    
    def add_event_ranges(self, eventRanges):
        self.jobs.process_event_ranges_reply(eventRanges)
    
    def assign_job_to_actor(self, actorID):
        jobID, nranges = self.jobs.jobid_next_job_to_process()
        self.actors[actorID] = jobID
        return self.jobs[jobID] if jobID else None
    
    def fetch_event_ranges(self, actorID, nranges):
        if actorID not in self.actors or not self.actors[actorID]:
            return list()

        ranges = self.jobs.get_eventranges(self.actors[actorID]).get_next_ranges(nranges)
        return ranges
    
    def process_event_ranges_update(self, actor_id, eventRangesUpdate):
        ranges_update = json.loads(eventRangesUpdate['eventRanges'][0])[0]
        actor_ranges = self.actors[actor_id].get_current_job_ranges()
        pandaID = self.actors[actor_id].get_current_pandaID()
        for r in ranges_update['eventRanges']:
            range_id = r['eventRangeID']
            if range_id not in actor_ranges:
                self.logging_actor.warn.remote("BookKeeper", f"{actor_id} sent update for {range_id} not attributed to it")
                continue
            status = r['eventStatus']
            if status == 'finished':
                self.logging_actor.info.remote("BookKeeper", f"{range_id} successfully processed by {actor_id}")
                processedrange = actor_ranges.pop(range_id)
                processedrange.set_done()
                self.finished_ranges.append(processedrange)
            else:
                self.logging_actor.info.remote("BookKeeper", f"{actor_id} encountered an error while processing {range_id}")
                failed_range = actor_ranges.pop(range_id)
                failed_range.retry += 1
                if failed_range.retry >= 3:
                    self.logging_actor.warn.remote("BookKeeper" f"{range_id} failed {failed_range.retry} times, dropping it")
                    failed_range.set_failed()
                    self.failed_ranges.append(failed_range)
                else:
                    self.event_ranges_available[pandaID].append(failed_range)

    def process_actor_end(self, actor_id):
        actor_ranges = self.actors[actor_id].get_current_job_ranges()
        pandaID = self.actors[actor_id].get_current_pandaID()
        self.logging_actor.warn.remote("BookKeeper", f"{actor_id} finished. {len(actor_ranges)} remaining to process")
        for rangeID, enventRange in actor_ranges.items():
            self.logging_actor.warn.remote("BookKeeper", f"{actor_id} finished without processing range {rangeID}")
            self.event_ranges_available[pandaID].append(enventRange)



class ESDriver(BaseDriver):

    def __init__(self, config):
        super().__init__(config)
        self.id = f"Driver_node:{get_node_ip()}"
        self.logging_actor = LoggingActor.remote(self.config)

        self.requestsQueue = Queue()
        self.jobQueue = Queue()
        self.eventRangesQueue = Queue()

        self.communicator_class = import_from_string(f"Raythena.drivers.communicators.{self.config.harvester['communicator']}")
        self.communicator = self.communicator_class(self.requestsQueue, self.jobQueue, self.eventRangesQueue, config)
        self.communicator.start()
        self.requestsQueue.put(PandaJobRequest())
        self.actors = dict()
        self.actors_message_queue = list()
        self.bookKeeper = BookKeeper(self.logging_actor, config)
        self.terminated = list()
        self.running = True

    def __str__(self):
        return self.__dict__.__str__()

    def __getitem__(self, key):
        return self.actors[key]

    def start_actors(self):
        """
        Initialize actor communication
        """
        for actor in self.actors.values():
            self.actors_message_queue.append(actor.get_message.remote())

    def create_actors(self):
        """
        Create new ray actors, one per node
        """
        nodes = build_nodes_resource_list(self.config)
        for node_constraint in nodes:
            _, _, nodeip = node_constraint.partition(':')
            actor_id = f"Actor_{nodeip}"
            actor_args = {
                'actor_id': actor_id,
                'panda_queue': self.bookKeeper.jobs[actor_id],
                'config': self.config,
                'logging_actor': self.logging_actor
            }
            actor = ESWorker._remote(num_cpus=self.config.resources.get('corepernode', psutil.cpu_count()), resources={node_constraint: 1}, kwargs=actor_args)
            self.actors[actor_id] = actor

    def handle_actors(self):

        new_messages, self.actors_message_queue = ray.wait(self.actors_message_queue)
        total_sent = 0
        while new_messages and self.running:
            for actor_id, message, data in ray.get(new_messages):
                if message == Messages.IDLE:
                    pass
                if message == Messages.REQUEST_NEW_JOB:
                    job = self.bookKeeper.assign_job_to_actor(actor_id)
                    self[actor_id].receive_job.remote(Messages.REPLY_OK if job else Messages.REPLY_NO_MORE_JOBS, job)
                elif message == Messages.REQUEST_EVENT_RANGES:
                    request = EventRangeRequest.build_from_dict(json.loads(data))
                    evt_range = self.bookKeeper.fetch_event_ranges(actor_id, request)
                    if evt_range:
                        total_sent += len(evt_range)
                        self.logging_actor.info.remote(self.id, f"sending {len(evt_range)} ranges to {actor_id}. Total sent: {total_sent} Remaining: {self.bookKeeper.get_nranges()}")
                    else:
                        self.logging_actor.info.remote(self.id, f"No more ranges to send to {actor_id}")
                    self[actor_id].receive_event_ranges.remote(Messages.REPLY_OK if evt_range else Messages.REPLY_NO_MORE_EVENT_RANGES, evt_range)
                elif message == Messages.UPDATE_JOB:
                    self.logging_actor.info.remote(self.id, f"{actor_id} sent a job update: {data}")
                elif message == Messages.UPDATE_EVENT_RANGES:
                    self.logging_actor.info.remote(self.id, f"{actor_id} sent a eventranges update: {data}")
                    self.bookKeeper.process_event_ranges_update(actor_id, data)
                elif message == Messages.PROCESS_DONE: #TODO actor should drain jobupdate, rangeupdate queue and send a final update.
                    self.terminated.append(actor_id)
                    self.bookKeeper.process_actor_end(actor_id)
                    # do not get new messages from this actor
                    continue

                self.actors_message_queue.append(self[actor_id].get_message.remote())
            new_messages, self.actors_message_queue = ray.wait(self.actors_message_queue)

    def cleanup(self):
        handles = list()
        for name, handle in self.actors.items():
            if name not in self.terminated:
                handles.append(handle.interrupt.remote())
                self.terminated.append(name)
        ray.get(handles)

    def run(self):
        self.logging_actor.info.remote(self.id, f"Started driver {self}")
        # gets initial jobs and send an eventranges request for each jobs
        jobs = self.jobQueue.get()
        if not jobs:
            self.logging_actor.critical.remote(self.id, "No jobs provided by communicator, stopping...")
            return

        self.bookKeeper.add_jobs(jobs)

        self.create_actors()

        evnt_request = EventRangeRequest()
        for pandaID in self.bookKeeper.jobs:
            cjob = self.bookKeeper.jobs[pandaID]
            evnt_request.add_event_request(pandaID, 100, cjob['taskID'], cjob['jobsetID'])
        self.requestsQueue.put(evnt_request)

        self.start_actors()

        self.handle_actors()

        self.communicator.stop()

    def stop(self):
        self.logging_actor.info.remote(self.id, "Graceful shutdown...")
        self.running = False
        self.cleanup()
