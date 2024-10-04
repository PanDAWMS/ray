import configparser
import contextlib
import json
import os
import shutil
import time
from queue import Queue

from raythena.drivers.communicators.baseCommunicator import BaseCommunicator
from raythena.utils.config import Config
from raythena.utils.eventservice import (
    EventRangeRequest,
    EventRangeUpdate,
    JobReport,
    PandaJobRequest,
    PandaJobUpdate,
)
from raythena.utils.exception import ExThread
from raythena.utils.logging import make_logger


class HarvesterFileCommunicator(BaseCommunicator):
    """
    This class implements the harvester communication protocol using shared file messenger
    as described at https://github.com/HSF/harvester/wiki/Agents-and-Plugins-descriptions#shared-file-messenger
    This communication protocol is typically used on HPC as harvester is running on edge nodes which can usually not
    communicate with compute nodes using http. As harvester and raythena are running on different nodes, a shared file
    system is required.
    """

    def __init__(
        self,
        requests_queue: Queue,
        job_queue: Queue,
        event_ranges_queue: Queue,
        config: Config,
    ) -> None:
        """
        Initialize communicator thread and parses the harvester config file

        Args:
            requests_queue: communication queue where requests are
            job_queue: queue used to place jobs retrieved by the communicator
            event_ranges_queue: queue used to place event ranges retrieved by the communicator
            config: app config
        """
        super().__init__(requests_queue, job_queue, event_ranges_queue, config)
        self.harvester_workdir = os.path.expandvars(
            self.config.harvester["endpoint"]
        )
        self.ranges_requests_count = 0
        self._parse_harvester_config()
        self.id = "HarvesterCommunicator"
        self._logger = make_logger(self.config, self.id)
        self.event_ranges_update_buffer = EventRangeUpdate()
        self.event_ranges_update_interval = 5 * 60
        self.communicator_thread = ExThread(
            target=self.run, name="communicator-thread"
        )

    def _parse_harvester_config(self) -> None:
        """
        Parses the harvester config file specified in the app config by the "harvester.harvesterconf" setting.
        Only the payload_interaction config section, which contains file names
        used to communicate with harvester is used. Each key in that config section is added as an attribute to
        this instance. 'jobspecfile', 'jobspecfile', 'eventrangesfile', 'eventrequestfile', 'eventstatusdumpjsonfile'
        should all be present in the harvester configuration file, a default value for these attributes is added if not
        found in the config file.

        Returns:
            None

        Raises:
            FileNotFoundError if the harvester config file doesn't exist
        """
        self.harvester_conf_file = os.path.expandvars(
            self.config.harvester["harvesterconf"]
        )
        if not os.path.isfile(self.harvester_conf_file):
            raise FileNotFoundError("Harvester config file not found")
        self.harvester_conf = configparser.ConfigParser()
        self.harvester_conf.read(self.harvester_conf_file)
        for k in self.harvester_conf["payload_interaction"]:
            setattr(
                self,
                k,
                os.path.join(
                    self.harvester_workdir,
                    self.harvester_conf["payload_interaction"][k],
                ),
            )
        if not hasattr(self, "jobspecfile"):
            self.jobspecfile = ""
        if not hasattr(self, "jobspecfile"):
            self.jobrequestfile = ""
        if not hasattr(self, "eventrangesfile"):
            self.eventrangesfile = ""
        if not hasattr(self, "eventrequestfile"):
            self.eventrequestfile = ""
        if not hasattr(self, "eventstatusdumpjsonfile"):
            self.eventstatusdumpjsonfile = ""
        if not hasattr(self, "jobreportfile"):
            self.jobreportfile = ""

    def request_job(self, request: PandaJobRequest) -> None:
        """
        Checks if a job is already provided by harvester. If not, write a jobrequestfile by dumping the request dict.
        Once the jobrequestfile is written to disk, blocks until harvester writes the jobspecfile then read it and put
        it in the job_queue. Also tries to delete jobspecfile and jobrequestfile after it's done retrieving a job.
        Args:
            request: Driver job request which triggered the call to this function.

        Returns:
            None
        """
        # Checks if a job file already exists
        if os.path.isfile(self.jobspecfile):
            with open(self.jobspecfile) as f:
                job = json.load(f)
                self.job_queue.put(job)
        else:
            # create request file if necessary
            if not os.path.isfile(self.jobrequestfile):
                request_tmp = f"{self.jobrequestfile}.tmp"
                with open(request_tmp, "w") as f:
                    json.dump(request.to_dict(), f)
                shutil.move(request_tmp, self.jobrequestfile)

            # wait on job file creation
            while not os.path.isfile(self.jobspecfile):
                time.sleep(0.01)

            # load job and remove request file
            with open(self.jobspecfile) as f:
                job = json.load(f)

        with contextlib.suppress(FileNotFoundError):
            os.remove(self.jobrequestfile)
        with contextlib.suppress(FileNotFoundError):
            os.rename(self.jobspecfile, f"{self.jobspecfile}.read")
        if job:
            self.job_queue.put(job)

    def request_event_ranges(self, request: EventRangeRequest) -> None:
        """
        Requests new event ranges to harvester. If no event ranges file and no event request file exist, write a new
        request file by dumping the request dict to file. If an eventrangesfile or an eventrequestfile already exists,
        the content of the request parameter will be ignored and the event ranges already available will be added
        to the ranges queue. Blocks until the eventrangesfile become available.
        Args:
            request: event request defining how many events to retrieve for each panda job worker_id

        Returns:
            None
        """
        if not os.path.isfile(self.eventrangesfile) and not os.path.exists(
            self.eventrequestfile
        ):
            event_request_file_tmp = f"{self.eventrequestfile}.tmp"
            with open(event_request_file_tmp, "w") as f:
                json.dump(request.request, f)
            shutil.move(event_request_file_tmp, self.eventrequestfile)
            self._logger.debug(
                f"request_event_ranges: created new {self.eventrequestfile} file"
            )

        while not os.path.isfile(self.eventrangesfile):
            time.sleep(1)

        self._logger.debug(
            f"request_event_ranges: found a {self.eventrangesfile} file"
        )
        while os.path.isfile(self.eventrangesfile):
            try:
                with open(self.eventrangesfile) as f:
                    ranges = json.load(f)
                if os.path.isfile(self.eventrangesfile):
                    shutil.move(
                        self.eventrangesfile,
                        f"{self.eventrangesfile}-{self.ranges_requests_count}",
                    )
            except Exception:
                time.sleep(5)
                if os.path.exists(
                    f"{self.eventrangesfile}-{self.ranges_requests_count}"
                ):
                    self.ranges_requests_count += 1

        with contextlib.suppress(FileNotFoundError):
            os.remove(self.eventrequestfile)

        self.ranges_requests_count += 1
        self.event_ranges_queue.put(ranges)

    def update_job(self, request: PandaJobUpdate) -> None:
        """
        Update job. As of now, job update are not sent to harvester.

        Args:
            request: the job status update sent by the driver

        Returns:
            None
        """
        pass

    def update_events(self, request: EventRangeUpdate) -> None:
        """
        Update event status to harvester. If an eventstatusdumpjsonfile already exists, block until harvester
        is done processing the update the send the current update.
        This function can be used to either update eventranges status or stage-out log files, depending on the format
        of the request parameter.

        Args:
            request: the event update to send to harvester

        Returns:
            None
        """
        self._logger.debug("Sending event ranges update to harvester...")
        tmp_status_dump_file = f"{self.eventstatusdumpjsonfile}.tmp"

        if os.path.isfile(tmp_status_dump_file):
            self._logger.debug("Cleanup leftover tmp file")
            try:
                with open(tmp_status_dump_file) as f:
                    current_update = json.load(f)
                os.remove(tmp_status_dump_file)
            except Exception as e:
                self._logger.critical(
                    "Failed to read and remove leftover tmp update file. Update will never get reported to harvester."
                )
                self._logger.critical(e)
            else:
                request.merge_update(EventRangeUpdate(current_update))

        self.merge_write_dump_file(request, tmp_status_dump_file)

        now = time.time()
        # eventstatusdumpjsonfile might be created by harvester, if this is the case, retry to merge it
        while os.path.isfile(self.eventstatusdumpjsonfile):
            self.merge_write_dump_file(request, tmp_status_dump_file)
            if time.time() - now > 60:
                break
            time.sleep(1)
        try:
            shutil.move(tmp_status_dump_file, self.eventstatusdumpjsonfile)
        except Exception as e:
            self._logger.critical(
                f"Failed to move temporary event status file to harvester dump file: {e}"
            )

    def merge_write_dump_file(
        self, request: EventRangeUpdate, tmp_status_dump_file: str
    ) -> None:
        if os.path.isfile(self.eventstatusdumpjsonfile):
            self._logger.debug(
                "Dump file already exists, merge with upcoming update"
            )
            try:
                shutil.move(self.eventstatusdumpjsonfile, tmp_status_dump_file)
                with open(tmp_status_dump_file) as f:
                    current_update = json.load(f)
            except Exception as e:
                self._logger.error(
                    f"Failed to move and load existing dump file: {e} "
                )
            else:
                request.merge_update(EventRangeUpdate(current_update))

        self._logger.debug("Writting event ranges update to temporary file")
        try:
            with open(tmp_status_dump_file, "w") as f:
                json.dump(request.range_update, f)
        except Exception as e:
            self._logger.error(
                f"Failed to write event update to temporary file: {e}"
            )

    def cleanup_tmp_files(self) -> None:
        tmp_status_dump_file = f"{self.eventstatusdumpjsonfile}.tmp"
        if os.path.isfile(tmp_status_dump_file):
            self._logger.warning(
                "About to quit with leftover temporary files... Last try to move it"
            )
            try:
                with open(tmp_status_dump_file) as f:
                    current_update = json.load(f)
                os.remove(tmp_status_dump_file)
            except Exception as e:
                self._logger.error(f"Failed: {e}")
            else:
                current_update = EventRangeUpdate(current_update)
                self.update_events(current_update)

    def create_job_report(self, request: JobReport) -> None:
        """
        Create a job report after the job has finished.

        Args:
            request: the job report to send to harvester

        Returns:
            None
        """
        job_report_file = f"{self.jobreportfile}"

        with open(job_report_file, "w") as f:
            json.dump(request.to_dict(), f)

    def run(self) -> None:
        """
        Target of the communicator thread. Wait for new requests from the driver by blocking on the queue.

        Returns:
            None
        """
        last_event_range_update = 0

        while True:
            try:
                request = self.requests_queue.get()
                if isinstance(request, PandaJobRequest):
                    self.request_job(request)
                elif isinstance(request, EventRangeRequest):
                    self.request_event_ranges(request)
                elif isinstance(request, PandaJobUpdate):
                    self.update_job(request)
                elif isinstance(request, EventRangeUpdate):
                    self.event_ranges_update_buffer.merge_update(request)
                    now = time.time()
                    if (
                        now - last_event_range_update
                        > self.event_ranges_update_interval
                    ):
                        self.update_events(self.event_ranges_update_buffer)
                        last_event_range_update = now
                        self.event_ranges_update_buffer = EventRangeUpdate()
                elif isinstance(request, JobReport):
                    self.create_job_report(request)
                else:  # if any other request is received, stop the thread
                    break
            except Exception as e:
                self._logger.error(
                    f"Exception occured while handling request: {e}"
                )

        if self.event_ranges_update_buffer:
            self.update_events(self.event_ranges_update_buffer)
        self.cleanup_tmp_files()

    def start(self) -> None:
        """
        Starts the communicator thread. The communicator needs to be stopped using stop() before it can be restarted.

        Returns:
            None
        """
        if not self.communicator_thread.is_alive():
            self.communicator_thread.start()

    def stop(self) -> None:
        """
        Join the communicator thread if still alive, blocking until the thread ends
        then create a new communicator thread

        Returns:
            None
        """
        if self.communicator_thread.is_alive():
            self.requests_queue.put(None)
            self.communicator_thread.join()
            self.communicator_thread = ExThread(
                target=self.run, name="communicator-thread"
            )
