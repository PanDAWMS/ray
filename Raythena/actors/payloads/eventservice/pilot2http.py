import asyncio
import functools
import json
import os
import stat
import shlex
import threading
from asyncio import Queue, QueueEmpty, Event
from subprocess import DEVNULL, Popen
from urllib.parse import parse_qs
from typing import Union, Dict, List

import uvloop
from aiohttp import web

from Raythena.utils.eventservice import ESEncoder
from Raythena.utils.exception import FailedPayload
from Raythena.actors.payloads.eventservice.esPayload import ESPayload
from Raythena.actors.loggingActor import LoggingActor
from Raythena.utils.config import Config
from Raythena.utils.eventservice import PandaJob, EventRange


class AsyncRouter:

    def __init__(self) -> None:
        self.routes = dict()

    def register(self, endpoint: str, handler: object):
        self.routes[endpoint] = handler

    async def route(self, endpoint: str, *args, **kwargs) -> web.Response:
        if self.routes.get(endpoint) is None:
            raise Exception(f"Route {endpoint} not registered")
        return await self.routes[endpoint](*args, **kwargs)


class Pilot2HttpPayload(ESPayload):

    def __init__(self, id: str, logging_actor: LoggingActor,
                 config: Config) -> None:
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
        self.stop_event = Event()
        self.ranges_update = Queue()
        self.job_update = Queue()
        self.ranges_queue = Queue()
        self.router = AsyncRouter()
        self.router.register('/', self.handle_get_job)
        self.router.register('/server/panda/getJob', self.handle_get_job)
        self.router.register('/server/panda/updateJob', self.handle_update_job)
        self.router.register('/server/panda/updateJobsInBulk',
                             self.handle_update_jobs_in_bulk)
        self.router.register('/server/panda/getStatus', self.handle_get_status)
        self.router.register('/server/panda/getEventRanges',
                             self.handle_get_event_ranges)
        self.router.register('/server/panda/updateEventRanges',
                             self.handle_update_event_ranges)
        self.router.register('/server/panda/getKeyPair',
                             self.handle_get_key_pair)

    def _start_payload(self) -> None:
        command = self._build_pilot_container_command(
        ) if self.config.payload.get('containerengine',
                                     None) else self._build_pilot_command()
        self.logging_actor.info.remote(self.id,
                                       f"Final payload command: {command}")
        # using PIPE will cause the subprocess to hang because
        # we're not reading data using communicate() and the pipe buffer becomes full as pilot2
        # generates a lot of data to the stdout pipe
        # see https://docs.python.org/3.7/library/subprocess.html#subprocess.Popen.wait
        self.pilot_process = Popen(command,
                                   stdin=DEVNULL,
                                   stdout=DEVNULL,
                                   stderr=DEVNULL,
                                   shell=True,
                                   close_fds=True)
        self.logging_actor.info.remote(
            self.id, f"Pilot payload started with PID {self.pilot_process.pid}")

    def _build_pilot_command(self) -> str:
        cmd = str()
        conda_activate = os.path.expandvars(
            os.path.join(self.config.payload.get('condabindir', ''),
                         'activate'))
        pilot_venv = self.config.payload.get('virtualenv', '')

        if os.path.isfile(conda_activate) and pilot_venv is not None:
            cmd += f"source {conda_activate} {pilot_venv};"

        extra_setup = self.config.payload.get('extrasetup', '')
        if extra_setup:
            cmd += f"{extra_setup}{';' if not extra_setup.endswith(';') else ''}"

        prodSourceLabel = shlex.quote(self.current_job['prodSourceLabel'])

        pilot_bin = os.path.expandvars(
            os.path.join(self.config.payload.get('bindir', ''), "pilot.py"))
        queue_escaped = shlex.quote(self.config.payload.get('pandaqueue', ''))
        payload_log = shlex.quote(
            self.config.payload.get('logfilename', 'wrapper'))
        # use exec to replace the shell process with python. Allows to send signal to the python process if needed
        cmd += f"python {shlex.quote(pilot_bin)} -q {queue_escaped} -r {queue_escaped} -s {queue_escaped} " \
               f"-i PR -j {prodSourceLabel} --pilot-user=ATLAS -t -w generic --url=http://{self.host} " \
               f"-p {self.port} --allow-same-user=False --resource-type MCORE > {payload_log} 2> {payload_log}.stderr;"

        extra_script = self.config.payload.get('extrapostpayload', '')
        if extra_script:
            cmd += f"{extra_script}{';' if not extra_script.endswith(';') else ''}"

        return cmd

    def _build_pilot_container_command(self) -> str:
        """
        """
        cmd = str()
        extra_setup = self.config.payload.get('extrasetup', '')
        if extra_setup:
            cmd += f"{extra_setup}{';' if not extra_setup.endswith(';') else ''}"

        pilot_src = f"{shlex.quote(self.config.ray['workdir'])}/pilot2"

        if not os.path.isdir(pilot_src):
            raise FailedPayload(self.id)

        cmd += f"ln -s {pilot_src} {os.path.join(os.getcwd(), 'pilot2')};"
        prodSourceLabel = shlex.quote(self.current_job['prodSourceLabel'])

        pilotwrapper_bin = os.path.expandvars(
            os.path.join(self.config.payload['bindir'], "runpilot2-wrapper.sh"))

        if not os.path.isfile(pilotwrapper_bin):
            raise FailedPayload(self.id)

        queue_escaped = shlex.quote(self.config.payload['pandaqueue'])
        cmd += f"{shlex.quote(pilotwrapper_bin)}  --piloturl local -q {queue_escaped} -r {queue_escaped} -s {queue_escaped} " \
               f"-i PR -j {prodSourceLabel} --container --mute --pilot-user=atlas -t -w generic --url=http://{self.host} " \
               f"-p {self.port} --allow-same-user=False --resource-type MCORE " \
               f"--hpc-resource {shlex.quote(self.config.payload['hpcresource'])};"

        extra_script = self.config.payload.get('extrapostpayload', '')
        if extra_script:
            cmd += f"{extra_script}{';' if not extra_script.endswith(';') else ''}"
        cmd_script = os.path.join(os.getcwd(), "payload.sh")
        with open(cmd_script, 'w') as f:
            f.write(cmd)
        st = os.stat(cmd_script)
        os.chmod(cmd_script, st.st_mode | stat.S_IEXEC)
        payload_log = shlex.quote(
            self.config.payload.get('logfilename', 'wrapper'))
        container = shlex.quote(self.config.payload.get('containerengine', ''))
        container_args = self.config.payload.get('containerextraargs', '')
        if container_args:
            container_args = shlex.quote(container_args)
        else:
            container_args = ''
        return f"{container} {container_args} /bin/bash {cmd_script} > {payload_log} 2> {payload_log}.stderr"

    def is_complete(self) -> bool:
        return self.pilot_process is not None and self.pilot_process.poll(
        ) is not None

    def return_code(self) -> int:
        return self.pilot_process.poll()

    def start(self, job: PandaJob) -> None:
        if not self.server_thread or not self.server_thread.is_alive():
            self.stop_event = Event()
            self.ranges_update = Queue()
            self.job_update = Queue()
            self.current_job = job
            self.ranges_queue = Queue()
            self.no_more_ranges = False
            self.server_thread = threading.Thread(target=self.run,
                                                  name="http-server")
            self.server_thread.start()

    def stop(self) -> None:
        if self.server_thread and self.server_thread.is_alive():

            pexit = self.pilot_process.poll()
            if pexit is None:
                self.pilot_process.terminate()
                pexit = self.pilot_process.wait()
            self.logging_actor.debug.remote(self.id,
                                            f"Payload return code: {pexit}")
            asyncio.run_coroutine_threadsafe(self.notify_stop_server_task(),
                                             self.loop)
            self.server_thread.join()
            self.logging_actor.info.remote(self.id, f"Communicator stopped")

    def submit_new_ranges(self, ranges: EventRange) -> None:
        asyncio.run_coroutine_threadsafe(self.ranges_queue.put(ranges),
                                         self.loop)

    def fetch_job_update(self) -> Union[None, Dict[str, str]]:
        try:
            return self.job_update.get_nowait()
        except QueueEmpty:
            return None

    def fetch_ranges_update(self) -> Union[None, Dict[str, str]]:
        try:
            return self.ranges_update.get_nowait()
        except QueueEmpty:
            return None

    def should_request_more_ranges(self) -> bool:
        if not self.ranges_queue:
            return True
        if self.no_more_ranges:
            return False
        return self.ranges_queue.qsize() == 0

    async def http_handler(self, request: web.BaseRequest) -> web.Response:
        self.logging_actor.debug.remote(
            self.id, f"Routing {request.method} {request.path}")
        try:
            return await self.router.route(request.path, request=request)
        except Exception:
            return web.json_response({"StatusCode": 500},
                                     dumps=self.json_encoder)

    @staticmethod
    async def parse_qs_body(request: web.BaseRequest) -> Dict[str, List[str]]:
        """
        Note: each value is packed in a list
        """
        body = dict()
        if request.can_read_body:
            body = await request.text()
            body = parse_qs(body)
        return body

    async def handle_get_job(self, request: web.BaseRequest) -> web.Response:
        job = self.current_job if self.current_job else dict()
        self.logging_actor.debug.remote(self.id, f"Serving job {job}")
        return web.json_response(job, dumps=self.json_encoder)

    async def handle_update_job(self, request: web.BaseRequest) -> web.Response:
        body = await Pilot2HttpPayload.parse_qs_body(request)
        await self.job_update.put(body)
        res = {"StatusCode": 0}
        return web.json_response(res, dumps=self.json_encoder)

    async def handle_get_event_ranges(self,
                                      request: web.BaseRequest) -> web.Response:
        body = await Pilot2HttpPayload.parse_qs_body(request)
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
        res = {"StatusCode": status, "eventRanges": ranges}
        self.logging_actor.info.remote(
            self.id, f"sending {len(res['eventRanges'])} ranges to pilot")
        return web.json_response(res, dumps=self.json_encoder)

    async def handle_update_event_ranges(
            self, request: web.BaseRequest) -> web.Response:
        body = await Pilot2HttpPayload.parse_qs_body(request)
        await self.ranges_update.put(body)
        res = {"StatusCode": 0}
        return web.json_response(res, dumps=self.json_encoder)

    async def handle_update_jobs_in_bulk(
            self, request: web.BaseRequest) -> web.Response:
        """
        Not used by pilot2
        """
        raise NotImplementedError(f"{request.path} handler not implemented")

    async def handle_get_status(self, request: web.BaseRequest) -> web.Response:
        """
        Not used by pilot2
        """
        raise NotImplementedError(f"{request.path} handler not implemented")

    async def handle_get_key_pair(self,
                                  request: web.BaseRequest) -> web.Response:
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
        self.logging_actor.debug.remote(
            self.id,
            f"======= Serving on http://{self.host}:{self.port}/ ======")
        return self.site

    async def notify_stop_server_task(self) -> None:
        self.stop_event.set()

    async def serve(self) -> None:
        await self.startup_server()
        self._start_payload()
        await self.stop_event.wait()
        if self.site:
            self.logging_actor.debug.remote(
                self.id,
                f"======= Stopped http://{self.host}:{self.port}/ ======")
            await self.site.stop()

    def run(self) -> None:
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self.serve())
