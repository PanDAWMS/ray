import asyncio
import functools
import json
import os
import shlex
import stat
from asyncio import Queue, QueueEmpty, Event
from subprocess import DEVNULL, Popen
from typing import Union, Dict, List, Callable
from urllib.parse import parse_qs

import uvloop
from aiohttp import web

from raythena.actors.logger import Logger
from raythena.actors.payloads.eventservice.esPayload import ESPayload
from raythena.utils.config import Config
from raythena.utils.eventservice import ESEncoder
from raythena.utils.eventservice import PandaJob, EventRange
from raythena.utils.exception import FailedPayload, ExThread


class AsyncRouter(object):
    """
    Very simple router mapping HTTP endpoint to a handler. Only supports with asynchronous handler compatible
    with the asyncio Framework.

    Usage:

        async def handle_http(*args, **kwargs):
            return "res"

        router = AsyncRouter()
        router.register("/", handle_http)
        res = await router.route("/")
    """

    def __init__(self) -> None:
        """
        Initialize attributes
        """
        self.routes = dict()

    def register(self, endpoint: str, handler: Callable):
        """
        Assign a handler to a http endpoint. If the http endpoint was already assigned, it is overrided

        Args:
            endpoint:
            handler: coroutine to be called when a http call is received

        Returns:
            None
        """
        self.routes[endpoint] = handler

    async def route(self, endpoint: str, *args, **kwargs) -> web.Response:
        """
        Call the http handler associated to the endpoint and return its result. *args and **kwargs are passed to the
        handler.

        Args:
            endpoint: http endpoint used to retrieve the handler
            *args: arguments passed to handler
            **kwargs: keywords arguments passed to handler

        Returns:
            the handler's return value

        Raises:
            Exception if the endpoint is not registered
        """
        if self.routes.get(endpoint) is None:
            raise Exception(f"Route {endpoint} not registered")
        return await self.routes[endpoint](*args, **kwargs)


class Pilot2HttpPayload(ESPayload):
    """
    This payload plugin processes panda jobs using the ray <-> pilot 2 <-> AthenMP workflow on HPC. The plugin starts
    a pilot process using Popen. Communication with the pilot process is done using HTTP using the same API
    specification provided by panda-server so that it doesn't require any change in pilot 2. The HTTP server is
    packaged in this class and started at the same time that the payload is started.

    """

    def __init__(self, worker_id: str, config: Config) -> None:
        """
        Setup initial state by creating queues, setting up the asyncio event loop, registering http endpoints
        and configuring the json encoder

        Args:
            worker_id: actor worker_id
            logging_actor: remote logger
            config: application config
        """
        super().__init__(worker_id, config)
        self._logger = Logger(self.config, self.worker_id)
        self.host = '127.0.0.1'
        self.port = 8080
        self.json_encoder = functools.partial(json.dumps, cls=ESEncoder)
        self.server_thread = None
        self.pilot_process = None
        self.site = None

        # prepare event loop for the server thread
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
        """
        Build the payload command and starts the pilot process using Popen

        Returns:
            None
        """
        command = self._build_pilot_container_command(
        ) if self.config.payload.get('containerengine',
                                     None) else self._build_pilot_command()
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
        self._logger.info(f"Pilot payload started with PID {self.pilot_process.pid}")

    def _build_pilot_command(self) -> str:
        """
        Build the payload command for environment without using a container. Typically used when the ray cluster
        itself is already running in a container.

        Returns:
            command string to execute
        """
        cmd = str()

        conda_activate = None
        condabindir = self.config.payload.get('condabindir', None)
        pilot_venv = self.config.payload.get('virtualenv', None)

        if condabindir is not None:
            conda_activate = os.path.expandvars(os.path.join(self.config.payload.get('condabindir', ''), 'activate'))

        if conda_activate is not None and os.path.isfile(conda_activate) and pilot_venv is not None:
            cmd += f"source {conda_activate} {pilot_venv};"

        extra_setup = self.config.payload.get('extrasetup', None)
        if extra_setup is not None:
            cmd += f"{extra_setup}{';' if not extra_setup.endswith(';') else ''}"

        pilot_version = self.config.payload.get('pilotversion', None)
        if pilot_version == 3:
            pilot_base = "pilot3"
            # pilot3 only supports python3, override conf
            py3pilot = True
        else:
            pilot_base = "pilot2"
            py3pilot = self.config.payload.get('py3pilot', None)

        pilot_src = f"{shlex.quote(self.config.ray['workdir'])}/{pilot_base}"

        if not os.path.isdir(pilot_src):
            raise FailedPayload(self.worker_id)

        cmd += f"ln -s {pilot_src} {os.path.join(os.getcwd(), pilot_base)};"

        prod_source_label = shlex.quote(self.current_job['prodSourceLabel'])

        pilotwrapper_bin = os.path.expandvars(
            os.path.join(self.config.payload['bindir'], "runpilot2-wrapper.sh"))

        if not os.path.isfile(pilotwrapper_bin):
            raise FailedPayload(self.worker_id)

        queue_escaped = shlex.quote(self.config.payload['pandaqueue'])
        cmd += f"{shlex.quote(pilotwrapper_bin)} --localpy --piloturl local -q {queue_escaped} -r {queue_escaped} -s {queue_escaped} "

        if pilot_version == 3:
            cmd += "--pilotversion 3 --pythonversion 3 "
        elif py3pilot:
            cmd += "--pythonversion 3 "

        cmd += f"-i PR -j {prod_source_label} --container --mute --pilot-user=atlas -t -u --es-executor-type=raythena -v 1 " \
            f"-d --cleanup=False -w generic --use-https False --allow-same-user=False --resource-type MCORE " \
            f"--hpc-resource {shlex.quote(self.config.payload['hpcresource'])};"

        extra_script = self.config.payload.get('extrapostpayload', None)
        if extra_script is not None:
            cmd += f"{extra_script}{';' if not extra_script.endswith(';') else ''}"
        cmd_script = os.path.join(os.getcwd(), "payload.sh")
        with open(cmd_script, 'w') as f:
            f.write(cmd)
        st = os.stat(cmd_script)
        os.chmod(cmd_script, st.st_mode | stat.S_IEXEC)
        payload_log = shlex.quote(
            self.config.payload.get('logfilename', 'wrapper'))
        return (f"/bin/bash {cmd_script} "
                f"> {payload_log} 2> {payload_log}.stderr")

    def _build_pilot_container_command(self) -> str:
        """
        Build the payload command to run pilot 2 in a container. Container engine used in defined in the config file.
        The payload command is written to a file which is then executed in the container environment

        Returns:
            command string to execute
        """
        cmd = str()
        extra_setup = self.config.payload.get('extrasetup', '')
        if extra_setup:
            cmd += f"{extra_setup}{';' if not extra_setup.endswith(';') else ''}"

        pilot_version = self.config.payload.get('pilotversion', None)
        if pilot_version == 3:
            pilot_base = "pilot3"
            # pilot3 only supports python3, override conf
            py3pilot = True
        else:
            pilot_base = "pilot2"
            py3pilot = self.config.payload.get('py3pilot', None)

        pilot_src = f"{shlex.quote(self.config.ray['workdir'])}/{pilot_base}"

        if not os.path.isdir(pilot_src):
            raise FailedPayload(self.worker_id)

        cmd += f"ln -s {pilot_src} {os.path.join(os.getcwd(), pilot_base)};"
        prod_source_label = shlex.quote(self.current_job['prodSourceLabel'])

        pilotwrapper_bin = os.path.expandvars(
            os.path.join(self.config.payload['bindir'], "runpilot2-wrapper.sh"))

        if not os.path.isfile(pilotwrapper_bin):
            raise FailedPayload(self.worker_id)

        queue_escaped = shlex.quote(self.config.payload['pandaqueue'])

        cmd += f"{shlex.quote(pilotwrapper_bin)} --localpy --piloturl local -q {queue_escaped} -r {queue_escaped} -s {queue_escaped} "

        if pilot_version == 3:
            cmd += "--pilotversion 3 --pythonversion 3 "
        elif py3pilot:
            cmd += "--pythonversion 3 "

        cmd += f"-i PR -j {prod_source_label} --container --mute --pilot-user=atlas -t -u --es-executor-type=raythena -v 1 " \
            f"-d --cleanup=False -w generic --url=http://{self.host} -p {self.port} --use-https False --allow-same-user=False --resource-type MCORE " \
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
        return (f"{container} {container_args} /bin/bash {cmd_script} "
                f"> {payload_log} 2> {payload_log}.stderr")

    def stagein(self) -> None:
        """
        Stage-in cric pandaqueues, queuedata and ddmendpoints info from harvester cacher

        Returns:
            None
        """
        cwd = os.getcwd()
        harvester_home = os.path.expandvars(self.config.harvester.get("cacher", ''))

        ddm_endpoints_file = os.path.join(harvester_home, "cric_ddmendpoints.json")
        if os.path.isfile(ddm_endpoints_file):
            os.symlink(ddm_endpoints_file, os.path.join(cwd, "cric_ddmendpoints.json"))

        pandaqueues_file = os.path.join(harvester_home, "cric_pandaqueues.json")
        if os.path.isfile(pandaqueues_file):
            os.symlink(pandaqueues_file, os.path.join(cwd, "cric_pandaqueues.json"))

        queue_escaped = shlex.quote(self.config.payload['pandaqueue'])
        queuedata_file = os.path.join(harvester_home, f"{queue_escaped}_queuedata.json")
        if os.path.isfile(queuedata_file):
            os.symlink(queuedata_file, os.path.join(cwd, "queuedata.json"))

    def stageout(self) -> None:
        """
        Pass, stage-out if performed on-the-fly by the worker after each event ranges update

        Returns:
            None
        """
        pass

    def is_complete(self) -> bool:
        """
        Checks if the payload subprocess ended.

        Returns:
            False if the payload has not finished yet, True otherwise
        """
        return self.pilot_process is not None and self.pilot_process.poll(
        ) is not None

    def return_code(self) -> int:
        """
        Returns the subprocess return code, or None is the subprocess hasn't finished yet

        Returns:
            None
        """
        return self.pilot_process.poll()

    def get_no_more_ranges(self) -> bool:
        """
        Returns the no_more_ranges bool

        Returns:
            None
        """
        return self.no_more_ranges

    def start(self, job: PandaJob) -> None:
        """
        Starts the payload subprocess and the http server in a separate thread

        Args:
            job: the job spec that should be processed by the payload

        Returns:
            None
        """
        if not self.server_thread or not self.server_thread.is_alive():
            self.stop_event = Event()
            self.ranges_update = Queue()
            self.job_update = Queue()
            self.current_job = job
            self.ranges_queue = Queue()
            self.no_more_ranges = False
            self.server_thread = ExThread(target=self.run,
                                          name="http-server")
            self.server_thread.start()

    def stop(self) -> None:
        """
        Stops the payload. If the subprocess hasn't finished yet, sends a SIGTERM signal to terminate it
        and wait until it exits then stop the http server

        Returns:
            None
        """
        if self.server_thread and self.server_thread.is_alive():

            pexit = self.pilot_process.poll()
            if pexit is None:
                self.pilot_process.terminate()
                pexit = self.pilot_process.wait()
            self._logger.debug(f"Payload return code: {pexit}")
            asyncio.run_coroutine_threadsafe(self.notify_stop_server_task(),
                                             self.loop)
            self.server_thread.join()

    def submit_new_range(self, event_range: Union[None, EventRange]) -> asyncio.Future:
        """
        Submits new event ranges to the payload thread but adding it to the event ranges queue

        Args:
            event_range: range to submit to the payload

        Returns:
            None
        """
        return asyncio.run_coroutine_threadsafe(self.ranges_queue.put(event_range),
                                                self.loop)

    def submit_new_ranges(self, event_ranges: Union[None, List[EventRange]]) -> None:
        futures = list()
        if event_ranges:
            for r in event_ranges:
                futures.append(self.submit_new_range(r))
        else:
            futures.append(self.submit_new_range(None))
        for fut in futures:
            fut.result()

    def fetch_job_update(self) -> Union[None, Dict[str, str]]:
        """
        Tries to get a job update from the payload by polling the job queue

        Returns:
            None if no job update update is available or a dict holding the update
        """
        try:
            res = self.job_update.get_nowait()
            # self._logger.debug(f"job update queue size is {self.job_update.qsize()}")
            return res
        except QueueEmpty:
            return None

    def fetch_ranges_update(self) -> Union[None, Dict[str, str]]:
        """
        Checks if event ranges update are available by polling the event ranges update queue

        Returns:
            Dict holding event range update of processed events, None if no update is available
        """
        try:
            res = self.ranges_update.get_nowait()
            # self._logger.debug(f"event ranges queue size is {self.ranges_update.qsize()}")
            return res
        except QueueEmpty:
            return None

    def should_request_more_ranges(self) -> bool:
        """
        Checks if the payload is ready to receive more event ranges

        Returns:
            True if the worker should send new event ranges to the payload, False otherwise
        """
        if not self.ranges_queue:
            return True
        if self.no_more_ranges:
            return False
        return self.ranges_queue.qsize() == 0

    async def http_handler(self, request: web.BaseRequest) -> web.Response:
        """
        Generic http handler dispatching the request to the handler associated the the request endpoint

        Args:
            request: http request received by the server

        Returns:
            response from the handler
        """
        try:
            return await self.router.route(request.path, request=request)
        except Exception:
            return web.json_response({"StatusCode": 500},
                                     dumps=self.json_encoder)

    @staticmethod
    async def parse_qs_body(request: web.BaseRequest) -> Dict[str, List[str]]:
        """
        Parses the query-string request body to a dictionary

        Args:
            request: http request received by the server

        Returns:
            dictionary holding the querystring keys-values
        """
        body = dict()
        if request.can_read_body:
            body = await request.text()
            body = parse_qs(body)
        return body

    async def handle_get_job(self, request: web.BaseRequest) -> web.Response:
        """
        Handler for getJob call, returns the panda job specifications

        Args:
            request : http request received by the server

        Returns:
            panda job specifications
        """
        del request
        job = self.current_job if self.current_job else dict()
        return web.json_response(job, dumps=self.json_encoder)

    async def handle_update_job(self, request: web.BaseRequest) -> web.Response:
        """
        Handler for updateJob call, adds the jobUpdate to a queue to be retrieved by the worker

        Args:
            request: http request received by the server

        Returns:
            Status code
        """
        body = await Pilot2HttpPayload.parse_qs_body(request)
        await self.job_update.put(body)
        res = {"StatusCode": 0}
        # self._logger.debug(f"job update queue size is {self.job_update.qsize()}")
        return web.json_response(res, dumps=self.json_encoder)

    async def handle_get_event_ranges(self,
                                      request: web.BaseRequest) -> web.Response:
        """
        Handler for getEventRanges call, retrieve event ranges from the queue and returns ranges to pilot 2.
        If not enough event ranges are available yet, wait until more ranges become available or a message indicating
        that no more ranges are available for this job, in that case

        Args:
            request: http request received by the server

        Returns:
            json holding the event ranges
        """
        body = await Pilot2HttpPayload.parse_qs_body(request)
        status = 0
        panda_id = body['pandaID'][0]
        ranges = list()
        # PandaID does not match the current job, return an error
        if panda_id != self.current_job['PandaID']:
            status = -1
        else:
            n_ranges = int(body['nRanges'][0])
            if not self.no_more_ranges:
                for i in range(n_ranges):
                    crange = await self.ranges_queue.get()
                    if crange is None:
                        self.no_more_ranges = True
                        break
                    ranges.append(crange)
        res = {"StatusCode": status, "eventRanges": ranges}
        # self._logger.info(f"{len(res['eventRanges'])} ranges sent to pilot")
        return web.json_response(res, dumps=self.json_encoder)

    async def handle_update_event_ranges(
            self, request: web.BaseRequest) -> web.Response:
        """
         Handler for updateEventRanges call, adds the event ranges update to a queue to be retrieved by the worker

        Args:
            request: http request received by the server

        Returns:
            status code
        """
        body = await Pilot2HttpPayload.parse_qs_body(request)
        await self.ranges_update.put(body)
        res = {"StatusCode": 0}
        # self._logger.debug(f"event ranges queue size is {self.ranges_update.qsize()}")
        return web.json_response(res, dumps=self.json_encoder)

    async def handle_update_jobs_in_bulk(
            self, request: web.BaseRequest) -> web.Response:
        """
        Not used by pilot 2

        Args:
            request: http request received by the server

        Returns:
            None

        Raises:
            NotImplementedError
        """
        raise NotImplementedError(f"{request.path} handler not implemented")

    async def handle_get_status(self, request: web.BaseRequest) -> web.Response:
        """
        Not used by pilot 2

        Args:
            request: http request received by the server

        Returns:
            None

        Raises:
            NotImplementedError
        """
        raise NotImplementedError(f"{request.path} handler not implemented")

    async def handle_get_key_pair(self,
                                  request: web.BaseRequest) -> web.Response:
        """
        Not used by pilot 2

        Args:
            request: http request received by the server

        Returns:
            None

        Raises:
            NotImplementedError
        """
        raise NotImplementedError(f"{request.path} handler not implemented")

    async def startup_server(self) -> web.TCPSite:
        """
        Starts the http server

        Returns:
            the TCP site holding socket information
        """
        server = web.Server(self.http_handler, access_log=None)
        runner = web.ServerRunner(server)
        await runner.setup()
        self.site = web.TCPSite(runner, self.host, self.port)
        await self.site.start()
        return self.site

    async def notify_stop_server_task(self) -> None:
        """
        Notify the server thread that the http server should stop listening

        Returns:
            None
        """
        self.stop_event.set()

    async def serve(self) -> None:
        """
        Starts the http server then blocks until it receives an end notification

        Returns:
            None
        """
        await self.startup_server()
        self._start_payload()
        await self.stop_event.wait()
        if self.site:
            await self.site.stop()

    def run(self) -> None:
        """
        Http server target method setting up the asyncio event loop for the current thread and blocks until the server
        is stopped

        Returns:
            None
        """
        asyncio.set_event_loop(self.loop)
        self.loop.run_until_complete(self.serve())
