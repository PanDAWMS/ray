from aiohttp import web
from .baseCommunicator import BaseCommunicator
from urllib.parse import parse_qs
from Raythena.utils.eventservice import EventRangeRequest, ESEncoder
from asyncio import Queue, QueueEmpty
import asyncio
import functools
import json
import threading
import uvloop


class AsyncRouter:

    def __init__(self):
        self.routes = dict()

    def register(self, endpoint, handler):
        self.routes[endpoint] = handler

    async def route(self, endpoint, *args, **kwargs):
        if self.routes.get(endpoint) is None:
            raise Exception(f"Route {endpoint} not registered")
        return await self.routes[endpoint](*args, **kwargs)


class Pilot2HttpCommunicator(BaseCommunicator):

    def __init__(self, actor, config):
        super().__init__(actor, config)
        self.host = '127.0.0.1'
        self.port = 8080
        self.json_encoder = functools.partial(json.dumps, cls=ESEncoder)
        self.actor.register_command_hook(self.fix_command)
        self.server_thread = None
        
        # prepare eventloop for the server thread
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        self.loop = asyncio.get_event_loop()

        self.no_more_jobs = False
        self.no_more_ranges = dict()
        self.stop_queue = Queue()
        self.job_queue = Queue()
        self.ranges_update = Queue()
        self.job_update = Queue()
        self.ranges_queue = dict()
        self.router = AsyncRouter()
        self.router.register('/server/panda/getJob', self.handle_getJob)
        self.router.register('/server/panda/updateJob', self.handle_updateJob)
        self.router.register('/server/panda/updateJobsInBulk', self.handle_updateJobsInBulk)
        self.router.register('/server/panda/getStatus', self.handle_getStatus)
        self.router.register('/server/panda/getEventRanges', self.handle_getEventRanges)
        self.router.register('/server/panda/updateEventRanges', self.handle_updateEventRanges)
        self.router.register('/server/panda/getKeyPair', self.handle_getkeyPair)

    def start(self):
        if not self.server_thread or not self.server_thread.is_alive():
            self.stop_queue = Queue()
            self.job_queue = Queue()
            self.ranges_update = Queue()
            self.job_update = Queue()
            self.ranges_queue = dict()
            self.no_more_ranges = dict()
            self.no_more_jobs = False
            self.server_thread = threading.Thread(target=self.run, name="http-server")
            self.server_thread.start()

    def stop(self):
        if self.server_thread and self.server_thread.is_alive():
            asyncio.run_coroutine_threadsafe(self.stop_queue.put(True), self.loop)
            self.server_thread.join()
            self.actor.logging_actor.info.remote(self.actor.id, f"Communicator stopped")

    def submit_new_job(self, job):
        asyncio.run_coroutine_threadsafe(self.job_queue.put(job), self.loop)

    def submit_new_ranges(self, pandaID, ranges):
        if pandaID not in self.ranges_queue:
            self.ranges_queue[pandaID] = Queue()
            self.no_more_ranges[pandaID] = False
        asyncio.run_coroutine_threadsafe(self.ranges_queue[pandaID].put(ranges), self.loop)

    def fetch_job_update(self):
        try:
            return self.job_update.get_nowait()
        except QueueEmpty as e:
            return None

    def fetch_ranges_update(self):
        try:
            return self.ranges_update.get_nowait()
        except QueueEmpty as e:
            return None
    
    def should_request_more_ranges(self, pandaID):
        if pandaID not in self.ranges_queue:
            return True
        if pandaID in self.no_more_ranges and self.no_more_ranges[pandaID]:
            return False
        return self.ranges_queue[pandaID].qsize() < self.config.resources['corepernode'] * 2

    def fix_command(self, command):
        return command

    async def http_handler(self, request: web.BaseRequest):
        self.actor.logging_actor.debug.remote(self.actor.id, f"Routing {request.method} {request.path}")
        return await self.router.route(request.path, request=request)

    async def parse_qs_body(self, request):
        """
        Note: each value is packed in a list
        """
        body = dict()
        if request.can_read_body:
            body = await request.text()
            body = parse_qs(body)
        return body

    async def handle_getJob(self, request):
        body = await self.parse_qs_body(request)
        job = dict()
        if not self.no_more_jobs:
            job = await self.job_queue.get()
            if job is None:
                self.no_more_jobs = True
                job = dict()
        self.actor.logging_actor.debug.remote(self.actor.id, f"Serving job {job}")
        return web.json_response(job)

    async def handle_updateJob(self, request):
        body = await self.parse_qs_body(request)
        await self.job_update.put(body)
        return web.json_response(body)

    async def handle_getEventRanges(self, request):
        body = await self.parse_qs_body(request)
        self.actor.logging_actor.debug.remote(self.actor.id, f"Body: {body}")
        status = 0
        pandaID = body['pandaID'][0]
        ranges = list()
        # No queue for the given panda id yet. Return error as we do not now if this pandaID will ever receive any range
        # 
        if pandaID not in self.ranges_queue:
            status = -1
        else:
            nranges = body['nRanges'][0]
            if not self.no_more_ranges[pandaID]:
                for i in range(nranges):
                    crange = await self.ranges_queue[pandaID].get()
                    if crange is None:
                        self.no_more_ranges[pandaID] = True
                        break
                    ranges.append(crange)
        res = {
            "StatusCode": status,
            "eventRanges": ranges
        }
        return web.json_response(res, dumps=self.json_encoder)

    async def handle_updateEventRanges(self, request):
        body = await self.parse_qs_body(request)
        await self.ranges_update.put(body)
        return web.json_response(body)

    async def handle_updateJobsInBulk(self, request):
        """
        Not used by pilot2
        """
        raise NotImplementedError(f"{request.path} handler not implemented")

    async def handle_getStatus(self, request):
        """
        Not used by pilot2
        """
        raise NotImplementedError(f"{request.path} handler not implemented")

    async def handle_getkeyPair(self, request):
        """
        Note used by pilot2
        """
        raise NotImplementedError(f"{request.path} handler not implemented")

    async def startup_server(self) -> web.TCPSite:
        server = web.Server(self.http_handler)
        runner = web.ServerRunner(server)
        await runner.setup()
        self.site = web.TCPSite(runner, self.host, self.port)
        await self.site.start()
        self.actor.logging_actor.debug.remote(self.actor.id, f"======= Serving on http://{self.host}:{self.port}/ ======")
        return self.site

    async def serve(self):
        await self.startup_server()

        should_stop = False
        while not should_stop:
            should_stop = await self.stop_queue.get()

        if self.site:
            self.actor.logging_actor.debug.remote(self.actor.id, f"======= Stopped http://{self.host}:{self.port}/ ======")
            await self.site.stop()

    def run(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self.serve())
