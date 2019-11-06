import logging
import ray
from Raythena.actors.pilot2Actor import Pilot2Actor
from Raythena.drivers.communicators.harvesterMock import HarvesterMock
from Raythena.utils.exception import BaseRaythenaException
from Raythena.utils.ray import build_nodes_resource_list
from .baseDriver import BaseDriver
logger = logging.getLogger(__name__)


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
        self.communicator = HarvesterMock(config)
        self.panda_queue = self.communicator.get_panda_queue_name()
        self.pilots = dict()

        self.jobs = list()
        self.request_new_job()
        self.jobs_finished = list()
        self.event_ranges_available = dict()
        self.request_new_event_ranges()

    def request_new_job(self):
        """
        Request new job to Harveste
        """
        job = self.communicator.get_job(dict())
        if job:
            self.jobs.append(job)

    def request_new_event_ranges(self):
        """
        Rquest new event ranges to harvester
        """
        new_ranges = self.communicator.get_event_ranges(dict())
        self.event_ranges_available.update(new_ranges)

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
        if self.jobs:
            self.pilots[pilot_id].add_job(self.jobs[0])
            return self.jobs

    def request_event_ranges_for_pilot(self, pilot_id, nranges):
        """
        Distribute event ranges to a pilot
        """
        new_ranges = list()
        for r in range(nranges):
            rangeId, evtrange = self.event_ranges_available.popitem()
            new_ranges.append(evtrange)
            self.pilots[pilot_id].event_ranges[rangeId] = evtrange
        return new_ranges


class Pilot2Driver(BaseDriver):

    def __init__(self, config):
        super().__init__(config)
        self.actors = dict()
        self.actors_message_queue = list()
        self.bookKeeper = BookKeeper(config)
        self.create_actors()
        self.running = True

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
            actor_id = f"Actor_{node_constraint}"
            actor_args = {
                'actor_id': actor_id,
                'panda_queue': self.bookKeeper.panda_queue,
                'config': self.config
            }
            actor = Pilot2Actor._remote(num_cpus=self.config.resource.get('core_per_node', 64), resources={node_constraint: 1}, kwargs=actor_args)
            self.bookKeeper.register_pilot_instance(actor_id)
            self.actors[actor_id] = actor

    def handle_actors(self):

        new_messages, self.actors_message_queue = ray.wait(self.actors_message_queue)

        while new_messages and self.running:
            for actor_id, message in ray.get(new_messages):
                logger.debug(f"got {message} from actor {actor_id}")
                if message == 0:
                    job = self.bookKeeper.request_job_for_pilot(actor_id)
                    self[actor_id].receive_job.remote(job)
                elif message == 1:
                    evt_range = self.bookKeeper.request_event_ranges_for_pilot(actor_id, 20)
                    self[actor_id].receive_event_ranges.remote(evt_range)
                elif message == 2:
                    self[actor_id].terminate_actor.remote()
                    continue
                self.actors_message_queue.append(self[actor_id].get_message.remote())
            new_messages, self.actors_message_queue = ray.wait(self.actors_message_queue)

    def cleanup(self):
        handles = list()
        for name, handle in self.actors.items():
            handles.append(handle.terminate_actor.remote())
        ray.get(handles)

    def run(self):
        logging.info(f"Started driver {self}")
        self.start_actors()

        self.handle_actors()

        self.cleanup()

    def stop(self):
        logger.info("Graceful shutdown...")
        self.running = False
