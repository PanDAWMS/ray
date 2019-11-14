import ray
import os
from Raythena.actors.pilot2Actor import Pilot2Actor
from Raythena.actors.loggingActor import LoggingActor
from Raythena.utils.exception import BaseRaythenaException
from Raythena.utils.ray import build_nodes_resource_list, get_node_ip, cluster_size
from Raythena.utils.importUtils import import_from_string
from Raythena.utils.eventservice import EventRangeRequest, Messages
from .baseDriver import BaseDriver
import psutil


class BookKeeper:

    class BookEntry:

        def __init__(self, pilot_id):
            self.pilot_id = pilot_id
            self.job = list()
            self.event_ranges = dict()

        def add_job(self, job):
            self.job.append(job)

        def add_event_range(self, rangeId, event_range):
            self.event_ranges[rangeId] = event_range

    def __init__(self, config):
        self.config = config
        self.communicator_class = import_from_string(f"Raythena.drivers.communicators.{self.config.harvester['communicator']}")
        self.communicator = self.communicator_class(config)
        self.panda_queue = self.communicator.get_panda_queue_name()
        self.pilots = dict()

        self.job = None
        self.request_new_job()
        self.jobs_finished = list()
        self.event_ranges_available = dict()
        self.request_new_event_ranges()

    def request_new_job(self):
        """
        Request new job to Harvester
        """
        self.job = self.communicator.get_job(dict())
        if self.job:
            job_dir = os.path.expandvars(os.path.join(self.config.pilot.get('workdir', os.getcwd()), self.job['PandaID']))
            if not os.path.exists(job_dir):
                os.makedirs(job_dir)

    def request_new_event_ranges(self):
        """
        Rquest new event ranges to harvester
        """
        new_ranges = self.communicator.get_event_ranges()
        if not new_ranges:
            return
        for pandaid, ranges in new_ranges.items():
            if pandaid not in self.event_ranges_available.keys():
                self.event_ranges_available[pandaid] = list()
            pandajob_ranges = self.event_ranges_available[pandaid]
            self.event_ranges_available[pandaid] = pandajob_ranges + ranges

    def register_pilot_instance(self, pilot_id):
        """
        Register a new pilot instance (actor) in the book keeping
        """
        if self.pilots.get(pilot_id, None) is not None:
            raise BaseRaythenaException()
        self.pilots[pilot_id] = BookKeeper.BookEntry(pilot_id)

    def request_job_for_pilot(self, pilot_id):
        """
        Allocate a new job to a pilot id
        """
        if self.job and self.job not in self.pilots[pilot_id].job:
            self.pilots[pilot_id].add_job(self.job)
            return self.job
        return None
    
    def get_nranges(self):
        return len(self.event_ranges_available.get(self.job['PandaID'], []))

    def request_event_ranges_for_pilot(self, pilot_id, eventRangeRequest: EventRangeRequest):
        """
        Distribute event ranges to a pilot
        """
        new_ranges = dict()
        for pandaID, request in eventRangeRequest.request.items():

            available_ranges = self.event_ranges_available.get(pandaID, list())
            nranges = int(request['nRanges'])
            if not available_ranges or len(available_ranges) < nranges:
                self.request_new_event_ranges()
                available_ranges = self.event_ranges_available.get(pandaID, None)
                if not available_ranges:
                    return dict()

            nranges = min(len(available_ranges), nranges)
            new_ranges[pandaID], self.event_ranges_available[pandaID] = available_ranges[:nranges], available_ranges[nranges:]
        return new_ranges


class Pilot2Driver(BaseDriver):

    def __init__(self, config):
        super().__init__(config)
        self.id = f"Driver_node:{get_node_ip()}"
        self.logging_actor = LoggingActor.remote(self.config)
        self.actors = dict()
        self.actors_message_queue = list()
        self.bookKeeper = BookKeeper(config)
        self.create_actors()
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
                'panda_queue': self.bookKeeper.panda_queue,
                'config': self.config,
                'logging_actor': self.logging_actor
            }
            actor = Pilot2Actor._remote(num_cpus=self.config.resources.get('corepernode', psutil.cpu_count()), resources={node_constraint: 1}, kwargs=actor_args)
            self.bookKeeper.register_pilot_instance(actor_id)
            self.actors[actor_id] = actor

    def handle_actors(self):

        new_messages, self.actors_message_queue = ray.wait(self.actors_message_queue)
        total_sent = 0
        while new_messages and self.running:
            for actor_id, message, data in ray.get(new_messages):
                if message == Messages.IDLE:
                    pass
                if message == Messages.REQUEST_NEW_JOB:
                    job = self.bookKeeper.request_job_for_pilot(actor_id)
                    self[actor_id].receive_job.remote(Messages.REPLY_OK if job else Messages.REPLY_NO_MORE_JOBS, job)
                elif message == Messages.REQUEST_EVENT_RANGES:
                    request = EventRangeRequest.build_from_json_string(data)
                    evt_range = self.bookKeeper.request_event_ranges_for_pilot(actor_id, request)
                    if evt_range:
                        values = list(evt_range.values())[0]
                        total_sent += len(values)
                        self.logging_actor.info.remote(self.id, f"sending {len(values)} ranges to {actor_id}. Total sent: {total_sent} Remaining: {self.bookKeeper.get_nranges()}")
                    else:
                        self.logging_actor.info.remote(self.id, f"No more ranges to send to {actor_id}")
                    self[actor_id].receive_event_ranges.remote(Messages.REPLY_OK if evt_range else Messages.REPLY_NO_MORE_EVENT_RANGES, evt_range)
                elif message == Messages.UPDATE_JOB:
                    self.logging_actor.info.remote(self.id, f"{actor_id} sent a job update: {data}")
                elif message == Messages.UPDATE_EVENT_RANGES:
                    self.logging_actor.info.remote(self.id, f"{actor_id} sent a eventranges update: {data}")
                elif message == Messages.PROCESS_DONE: #TODO actor should drain jobupdate, rangeupdate queue and send a final update.
                    self.terminated.append(actor_id)
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
        self.start_actors()

        self.handle_actors()

    def stop(self):
        self.logging_actor.info.remote(self.id, "Graceful shutdown...")
        self.running = False
        self.cleanup()
