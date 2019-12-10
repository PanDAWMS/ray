import asyncio
import functools
import json
import os
import stat
import shlex
import threading
from asyncio import Queue, QueueEmpty
from subprocess import DEVNULL, Popen
from urllib.parse import parse_qs

import uvloop
from aiohttp import web

from Raythena.utils.eventservice import ESEncoder
from Raythena.utils.exception import FailedPayload
from Raythena.actors.payloads.eventservice.esPayload import ESPayload


class AsyncRouter:

    def __init__(self):
        self.routes = dict()

    def register(self, endpoint, handler):
        self.routes[endpoint] = handler

    async def route(self, endpoint, *args, **kwargs):
        if self.routes.get(endpoint) is None:
            raise Exception(f"Route {endpoint} not registered")
        return await self.routes[endpoint](*args, **kwargs)


class Pilot2HttpPayload(ESPayload):

    def __init__(self, id, logging_actor, config):
        super().__init__(id, logging_actor, config)
        self.host = '127.0.0.1'
        self.port = 8080
        self.json_encoder = functools.partial(json.dumps, cls=ESEncoder)
        self.server_thread = None
        self.pilot_process = None

        # prepare eventloop for the server thread
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
        self.loop = asyncio.get_event_loop()
        self.current_job = None
        self.no_more_ranges = False
        self.stop_queue = Queue()
        self.ranges_update = Queue()
        self.job_update = Queue()
        self.ranges_queue = Queue()
        self.router = AsyncRouter()
        self.router.register('/server/panda/getJob', self.handle_getJob)
        self.router.register('/server/panda/updateJob', self.handle_updateJob)
        self.router.register('/server/panda/updateJobsInBulk', self.handle_updateJobsInBulk)
        self.router.register('/server/panda/getStatus', self.handle_getStatus)
        self.router.register('/server/panda/getEventRanges', self.handle_getEventRanges)
        self.router.register('/server/panda/updateEventRanges', self.handle_updateEventRanges)
        self.router.register('/server/panda/getKeyPair', self.handle_getkeyPair)

    def _start_payload(self):
        command = self._build_pilot_command()
        self.logging_actor.info.remote(self.id, f"Final payload command: {command}")
        # using PIPE will cause the subprocess to hang because
        # we're not reading data using communicate() and the pipe buffer becomes full as pilot2
        # generates a lot of data to the stdout pipe
        # see https://docs.python.org/3.7/library/subprocess.html#subprocess.Popen.wait
        self.pilot_process = Popen(command, stdin=DEVNULL, stdout=DEVNULL, stderr=DEVNULL, shell=True, close_fds=True)
        self.logging_actor.info.remote(self.id, f"Pilot payload started with PID {self.pilot_process.pid}")

    def _build_pilot_command(self):
        """
        """
        cmd = str()
        os.path.dirname(os.getcwd())
        extra_setup = self.config.payload.get('extrasetup', '')
        if extra_setup:
            cmd += f"{extra_setup}{';' if not extra_setup.endswith(';') else ''}"

        pilot_src = f"{shlex.quote(self.config.ray['workdir'])}/pilot2"

        if not os.path.isdir(pilot_src):
            raise FailedPayload(self.id)

        cmd += f"ln -s {pilot_src} {os.path.join(os.getcwd(), 'pilot2')};"
        prodSourceLabel = shlex.quote(self.current_job['prodSourceLabel'])

        pilotwrapper_bin = os.path.expandvars(os.path.join(self.config.payload['bindir'], "runpilot2-wrapper.sh"))

        if not os.path.isfile(pilotwrapper_bin):
            raise FailedPayload(self.id)

        queue_escaped = shlex.quote(self.config.payload['pandaqueue'])
        cmd += f"{shlex.quote(pilotwrapper_bin)}  --piloturl local -q {queue_escaped} -r {queue_escaped} -s {queue_escaped} " \
               f"-i PR -j {prodSourceLabel} --container --mute --pilot-user=atlas -t -w generic --url=http://{self.host} " \
               f"-p {self.port} --allow-same-user=False --resource-type MCORE --hpc-resource {shlex.quote(self.config.payload['hpcresource'])};"

        extra_script = self.config.payload.get('extrapostpayload', '')
        if extra_script:
            cmd += f"{extra_script}{';' if not extra_script.endswith(';') else ''}"
        cmd_script = os.path.join(os.getcwd(), "payload.sh")
        with open(cmd_script, 'w') as f:
            f.write(cmd)
        st = os.stat(cmd_script)
        os.chmod(cmd_script, st.st_mode | stat.S_IEXEC)
        payload_log = shlex.quote(self.config.payload.get('logfilename', 'wrapper'))
        container = shlex.quote(self.config.payload.get('containerengine', ''))
        container_args = self.config.payload.get('containerextraargs', '')
        if container_args:
            container_args = shlex.quote(container_args)
        else:
            container_args = ''
        return f"{container} {container_args} /bin/bash {cmd_script} > {payload_log} 2> {payload_log}.stderr"

    def is_complete(self):
        return self.pilot_process is not None and self.pilot_process.poll() is not None

    def returncode(self):
        return self.pilot_process.poll()

    def start(self, job):
        if not self.server_thread or not self.server_thread.is_alive():
            self.stop_queue = Queue()
            self.ranges_update = Queue()
            self.job_update = Queue()
            self.current_job = job
            self.ranges_queue = Queue()
            self.no_more_ranges = False
            self.server_thread = threading.Thread(target=self.run, name="http-server")
            self.server_thread.start()

    def stop(self):
        if self.server_thread and self.server_thread.is_alive():

            pexit = self.pilot_process.poll()
            if pexit is None:
                self.pilot_process.terminate()
                pexit = self.pilot_process.wait()
            self.logging_actor.debug.remote(self.id, f"Payload return code: {pexit}")

            asyncio.run_coroutine_threadsafe(self.stop_queue.put(True), self.loop)
            self.server_thread.join()
            self.logging_actor.info.remote(self.id, f"Communicator stopped")

    def submit_new_ranges(self, ranges):
        asyncio.run_coroutine_threadsafe(self.ranges_queue.put(ranges), self.loop)

    def fetch_job_update(self):
        try:
            return self.job_update.get_nowait()
        except QueueEmpty:
            return None

    def fetch_ranges_update(self):
        try:
            return self.ranges_update.get_nowait()
        except QueueEmpty:
            return None

    def should_request_more_ranges(self):
        if not self.ranges_queue:
            return True
        if self.no_more_ranges:
            return False
        return self.ranges_queue.qsize() == 0

    async def http_handler(self, request: web.BaseRequest):
        self.logging_actor.debug.remote(self.id, f"Routing {request.method} {request.path}")
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
        job = self.current_job if self.current_job else dict()
        self.logging_actor.debug.remote(self.id, f"Serving job {job}")
        return web.json_response(job, dumps=self.json_encoder)

    async def handle_updateJob(self, request):
        body = await self.parse_qs_body(request)
        await self.job_update.put(body)
        res = {
            "StatusCode": 0
        }
        return web.json_response(res, dumps=self.json_encoder)

    async def handle_getEventRanges(self, request):
        body = await self.parse_qs_body(request)
        self.logging_actor.debug.remote(self.id, f"Body: {body}")
        status = 0
        pandaID = body['pandaID'][0]
        ranges = list()
        # PandaID does not match the current job, return an error
        if pandaID != self.current_job['PandaID']:
            status = -1
        else:
            nranges = int(body['nRanges'][0])
            if not self.no_more_ranges:
                for i in range(nranges):
                    crange = await self.ranges_queue.get()
                    if crange is None:
                        self.no_more_ranges = True
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
        res = {
            "StatusCode": 0
        }
        return web.json_response(res, dumps=self.json_encoder)

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
        self.logging_actor.debug.remote(self.id, f"======= Serving on http://{self.host}:{self.port}/ ======")
        return self.site

    async def serve(self):
        await self.startup_server()
        self._start_payload()
        should_stop = False
        while not should_stop:
            should_stop = await self.stop_queue.get()

        if self.site:
            self.logging_actor.debug.remote(self.id, f"======= Stopped http://{self.host}:{self.port}/ ======")
            await self.site.stop()

    def run(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self.serve())
