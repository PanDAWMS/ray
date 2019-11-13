from aiohttp import web
from .baseCommunicator import BaseCommunicator
from urllib.parse import parse_qs
from Raythena.utils.eventservice import EventRangeRequest, ESEncoder
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
        self.should_stop = False

        self.router = AsyncRouter()
        self.router.register('/server/panda/getJob', self.handle_getJob)
        self.router.register('/server/panda/updateJob', self.handle_updateJob)
        self.router.register('/server/panda/updateJobsInBulk', self.handle_updateJobsInBulk)
        self.router.register('/server/panda/getStatus', self.handle_getStatus)
        self.router.register('/server/panda/getEventRanges', self.handle_getEventRanges)
        self.router.register('/server/panda/updateEventRanges', self.handle_updateEventRanges)
        self.router.register('/server/panda/getKeyPair', self.handle_getkeyPair)

    def start(self):
        self.should_stop = False
        self.server_thread = threading.Thread(target=self.run, name="http-server")
        self.server_thread.start()

    def stop(self):
        self.should_stop = True
        self.server_thread.join()

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
        self.actor.logging_actor.debug.remote(self.actor.id, f"Body: {body}")
        self.actor.logging_actor.debug.remote(self.actor.id, f"Serving job {self.actor.job}")
        return web.json_response(self.actor.job)

    async def handle_updateJob(self, request):
        body = await self.parse_qs_body(request)
        self.actor.logging_actor.debug.remote(self.actor.id, f"Body: {body}")
        return web.json_response(body)

    async def handle_updateJobsInBulk(self, request):
        raise NotImplementedError(f"{request.path} handler not implemented")

    async def handle_getStatus(self, request):
        raise NotImplementedError(f"{request.path} handler not implemented")

    async def handle_getEventRanges(self, request):
        body = await self.parse_qs_body(request)
        req = EventRangeRequest()
        self.actor.logging_actor.debug.remote(self.actor.id, f"Body: {body}")
        req.add_event_request(body['pandaID'][0], body['nRanges'][0], body['taskID'][0], body['jobsetID'][0])
        ranges = self.actor.get_ranges(req)
        status_code = 0
        if not ranges:
            ranges = list()
        res = {
            "StatusCode": status_code,
            "eventRanges": ranges
        }
        return web.json_response(res, dumps=self.json_encoder)

    async def handle_updateEventRanges(self, request):
        body = await self.parse_qs_body(request)
        self.actor.logging_actor.info.remote(self.actor.id, f"EventRange update: {body}")
        return web.json_response(body)

    async def handle_getkeyPair(self, request):
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

        while not self.should_stop:
            await asyncio.sleep(60)

        if self.site:
            self.actor.logging_actor.debug.remote(self.actor.id, f"======= Stopped http://{self.host}:{self.port}/ ======")
            await self.site.stop()

    def run(self):
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        self.loop = asyncio.new_event_loop()
        self.loop.run_until_complete(self.serve())
