import hashlib
import os
import random
import time
from queue import Queue
from raythena.drivers.communicators.baseCommunicator import BaseCommunicator
from raythena.utils.config import Config
from raythena.utils.eventservice import (
    EventRangeRequest,
    EventRangeUpdate,
    PandaJobRequest,
    PandaJobUpdate,
)
from raythena.utils.exception import ExThread


class HarvesterMock(BaseCommunicator):
    """
    This class is mostly used for testing purposes. It provides the driver thread with a
    sample panda jobspec for Athena/21.0.15 and a predefined number of event ranges.
    Messages exchanged with this class follows the same format as harvester, it expects the same request format
    and returns the same response format as harvester would.

    Input files specified in the inFiles attribute should exist in the ray workdir before starting ray
    """

    def __init__(
        self,
        requests_queue: Queue,
        job_queue: Queue,
        event_ranges_queue: Queue,
        config: Config,
    ) -> None:
        super().__init__(requests_queue, job_queue, event_ranges_queue, config)
        """
        Initialize communicator thread, input files name, job worker_id, number of events to be distributed
        """
        self.communicator_thread = ExThread(
            target=self.run, name="communicator-thread"
        )
        self.event_ranges = None
        self.pandaID = random.randint(0, 100)
        self.jobsetId = random.randint(0, 100)
        self.taskId = random.randint(0, 100)
        self.config = config
        self.scope = "mc16_13TeV"
        self.guid = "74DFB3ED-DAA7-E011-8954-001E4F3D9CB1,74DFB3ED-DAA7-E011-8954-001E4F3D9CB1"
        self.guids = self.guid.split(",")
        self.inFiles = "EVNT.12458444._000048.pool.root.1,EVNT.12458444._000052.pool.root.1"
        workdir = os.path.expandvars(self.config.ray["workdir"])
        self.files = self.inFiles.split(",")
        self.nfiles = len(self.files)
        self.inFilesAbs = list()
        for f in self.files:
            self.inFilesAbs.append(os.path.join(workdir, f))

        self.nevents_per_file = 5000
        self.nevents = self.nevents_per_file * self.nfiles
        self.served_events = 0
        self.ncores = self.config.resources["corepernode"]

    def run(self) -> None:
        """
        Target of the communicator thread. Wait for new requests from the driver by blocking on the queue.

        Returns:
            None
        """
        while True:
            request = self.requests_queue.get()
            if isinstance(request, PandaJobRequest):
                self.request_job(request)
            elif isinstance(request, EventRangeRequest):
                self.request_event_ranges(request)
            elif isinstance(request, PandaJobUpdate):
                self.update_job(request)
            elif isinstance(request, EventRangeUpdate):
                self.update_events(request)
            else:  # if any other request is received, stop the thread
                break

    def start(self) -> None:
        """
        Starts the communicator thread. Can only be used once, a new object will need to bo recreated
        if the communicator needs to be restarted.

        Returns:
            None
        """
        self.communicator_thread.start()

    def stop(self) -> None:
        """
        Join the communicator thread, blocking until the thread ends.

        Returns:
            None
        """
        self.requests_queue.put(None)
        self.communicator_thread.join()

    def request_event_ranges(self, request: EventRangeRequest) -> None:
        """
        Provides event ranges. Ranges will be provided for each job worker_id in the request. At most self.nevents
        will be provided in total, also counting previous request and ranges provided to different job ids.

        Args:
            request:

        Returns:
            None
        """
        self.event_ranges = dict()

        for pandaID in request:
            self.event_ranges[pandaID] = list()

        if self.served_events >= self.nevents:
            self.event_ranges_queue.put(self.event_ranges)
            return

        for pandaID in request:
            range_list = list()
            request_dict = request[pandaID]
            nranges = min(
                self.nevents - self.served_events, request_dict["nRanges"]
            )
            for i in range(
                self.served_events + 1, self.served_events + nranges + 1
            ):
                file_idx = self.served_events // self.nevents_per_file
                range_id = f"Range-{i:05}"
                range_list.append(
                    {
                        "lastEvent": i - file_idx * self.nevents_per_file,
                        "eventRangeID": range_id,
                        "startEvent": i - file_idx * self.nevents_per_file,
                        "scope": self.scope,
                        "LFN": self.inFilesAbs[file_idx],
                        "GUID": self.guids[file_idx],
                    }
                )

                self.served_events += 1

            self.event_ranges[pandaID] = range_list
        self.event_ranges_queue.put(self.event_ranges)

    def update_job(self, job_status: PandaJobUpdate) -> None:
        """
        Update job. Not necessary without harvester
        Args:
            job_status: the job status update sent by the driver

        Returns:
            None
        """
        pass

    def update_events(self, evnt_status: EventRangeUpdate) -> None:
        """
        Update events. Not necessary without harvester
        Args:
            evnt_status: the event status update sent by the driver

        Returns:
            None
        """
        pass

    def get_panda_queue_name(self) -> str:
        """
        Returns pandaqueue name set in the config file.

        Returns:
            The name of the pandaqueue from which jobs are retrieved.
        """
        return self.config.payload["pandaqueue"]

    def request_job(self, job_request: PandaJobRequest) -> None:
        """
        Default job provided to the driver. The job spec is added to the job_queue so that other threads
        can retrieve it.

        Args:
            job_request: Ignored. Driver job request which triggered the call to this function

        Returns:
            None
        """
        md5_hash = hashlib.md5()

        md5_hash.update(str(time.time()).encode("utf-8"))
        log_guid = md5_hash.hexdigest()

        md5_hash.update(str(time.time()).encode("utf-8"))
        job_name = md5_hash.hexdigest()

        self.job_queue.put(
            {
                str(self.pandaID): {
                    "jobsetID": self.jobsetId,
                    "logGUID": log_guid,
                    "cmtConfig": "x86_64-slc6-gcc49-opt",
                    "prodDBlocks": "user.mlassnig:user.mlassnig.pilot.test.single.hits",
                    "dispatchDBlockTokenForOut": "NULL,NULL",
                    "destinationDBlockToken": "NULL,NULL",
                    "destinationSE": self.get_panda_queue_name(),
                    "realDatasets": job_name,
                    "prodUserID": "no_one",
                    "GUID": self.guid,
                    "realDatasetsIn": "user.mlassnig:user.mlassnig.pilot.test.single.hits",
                    "nSent": 0,
                    "eventService": "true",
                    "cloud": "US",
                    "StatusCode": 0,
                    "homepackage": "AtlasOffline/21.0.15",
                    "inFiles": self.inFiles,
                    "processingType": "pilot-ptest",
                    "ddmEndPointOut": "UTA_SWT2_DATADISK,UTA_SWT2_DATADISK",
                    "fsize": "118612262",
                    "fileDestinationSE": f"{self.get_panda_queue_name()},{self.get_panda_queue_name()}",
                    "scopeOut": "panda",
                    "minRamCount": 0,
                    "jobDefinitionID": 7932,
                    "maxWalltime": "NULL",
                    "scopeLog": "panda",
                    "transformation": "Sim_tf.py",
                    "maxDiskCount": 0,
                    "coreCount": self.ncores,
                    "prodDBlockToken": "NULL",
                    "transferType": "NULL",
                    "destinationDblock": job_name,
                    "dispatchDBlockToken": "NULL",
                    "jobPars": (
                        "--eventService=True --skipEvents=0 --firstEvent=1 --preExec 'from AthenaCommon.DetFlags "
                        "import DetFlags;DetFlags.ID_setOn();DetFlags.Calo_setOff();"
                        "DetFlags.Muon_setOff();DetFlags.Lucid_setOff();DetFlags.Truth_setOff()' "
                        "--athenaopts=--preloadlib=${ATLASMKLLIBDIR_PRELOAD}/libimf.so "
                        "--preInclude sim:SimulationJobOptions/preInclude.FrozenShowersFCalOnly.py,"
                        "SimulationJobOptions/preInclude.BeamPipeKill.py "
                        "--geometryVersion ATLAS-R2-2016-01-00-00_VALIDATION --physicsList QGSP_BERT "
                        "--randomSeed 1234 --conditionsTag OFLCOND-MC12-SIM-00 "
                        f"--maxEvents=-1 --inputEvgenFile {self.inFiles} --outputHitsFile HITS_{job_name}.pool.root"
                    ),
                    "attemptNr": 0,
                    "swRelease": "Atlas-21.0.15",
                    "nucleus": "NULL",
                    "maxCpuCount": 0,
                    "outFiles": f"HITS_{job_name}.pool.root,{job_name}.job.log.tgz",
                    "currentPriority": 1000,
                    "scopeIn": self.scope,
                    "PandaID": self.pandaID,
                    "sourceSite": "NULL",
                    "dispatchDblock": "NULL",
                    "prodSourceLabel": "ptest",
                    "checksum": "ad:5d000974",
                    "jobName": job_name,
                    "ddmEndPointIn": "UTA_SWT2_DATADISK",
                    "taskID": self.taskId,
                    "logFile": f"{job_name}.job.log.tgz",
                }
            }
        )
