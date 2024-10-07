import hashlib
import os
import random
import time
from queue import Queue
from raythena.drivers.communicators.harvesterMock import HarvesterMock
from raythena.utils.config import Config
from raythena.utils.eventservice import PandaJobRequest
from raythena.utils.exception import ExThread


class HarvesterMock2205(HarvesterMock):
    """
    Same purposes as HarvesterMock except that a job spec for Athena/22.0.5 is provided
    """

    def __init__(
        self,
        requests_queue: Queue,
        job_queue: Queue,
        event_ranges_queue: Queue,
        config: Config,
    ) -> None:
        """
        Initialize communicator thread, input files name, job worker_id, number of events to be distributed
        """
        super().__init__(requests_queue, job_queue, event_ranges_queue, config)
        self.communicator_thread = ExThread(target=self.run, name="communicator-thread")
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

        self.nevents_per_file = 50
        self.nevents = self.nevents_per_file * self.nfiles
        self.served_events = 0
        self.ncores = self.config.resources["corepernode"]

    def request_job(self, job_request: PandaJobRequest) -> None:
        """
        Default job provided to the diver. The job spec is added to the job_queue so that other threads
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
                    "cmtConfig": "x86_64-centos7-gcc8-opt",
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
                    "homepackage": "Athena/22.0.5",
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
                        "--multiprocess --eventService=True --skipEvents=0 --firstEvent=1 "
                        "--preExec 'from AthenaCommon.DetFlags "
                        "import DetFlags;DetFlags.ID_setOn();DetFlags.Calo_setOff();"
                        "DetFlags.Muon_setOff();DetFlags.Lucid_setOff();DetFlags.Truth_setOff()' "
                        "--athenaopts=--preloadlib=${ATLASMKLLIBDIR_PRELOAD}/libimf.so "
                        "--preInclude sim:SimulationJobOptions/preInclude.FrozenShowersFCalOnly.py,"
                        "SimulationJobOptions/preInclude.BeamPipeKill.py "
                        "--geometryVersion default:ATLAS-R2-2016-01-00-01_VALIDATION "
                        "--physicsList FTFP_BERT_ATL_VALIDATION --randomSeed 1234 "
                        "--conditionsTag default:OFLCOND-MC16-SDR-14 "
                        f"--maxEvents=-1 --inputEvgenFile {self.inFiles} --outputHitsFile HITS_{job_name}.pool.root"
                    ),
                    "attemptNr": 0,
                    "swRelease": "Atlas-22.0.5",
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
