import hashlib
import os
import time
import random
import threading

from Raythena.drivers.communicators.harvesterMock import HarvesterMock


class HarvesterMock2205(HarvesterMock):

    def __init__(self, requestsQueue, jobQueue, eventRangesQueue, config):
        super().__init__(requestsQueue, jobQueue, eventRangesQueue, config)
        self.communicator_thread = threading.Thread(target=self.run, name="communicator-thread")
        self.event_ranges = None
        self.pandaID = random.randint(0, 100)
        self.jobsetId = random.randint(0, 100)
        self.taskId = random.randint(0, 100)
        self.config = config
        self.scope = 'mc16_13TeV'
        self.guid = '74DFB3ED-DAA7-E011-8954-001E4F3D9CB1,74DFB3ED-DAA7-E011-8954-001E4F3D9CB1'
        self.guids = self.guid.split(",")
        self.inFiles = "EVNT.12458444._000048.pool.root.1,EVNT.12458444._000052.pool.root.1"
        workdir = os.path.expandvars(self.config.ray['workdir'])
        self.files = self.inFiles.split(",")
        self.nfiles = len(self.files)
        self.inFilesAbs = list()
        for f in self.files:
            self.inFilesAbs.append(os.path.join(workdir, f))

        self.nevents_per_file = 50
        self.nevents = self.nevents_per_file * self.nfiles
        self.served_events = 0
        self.ncores = self.config.resources['corepernode']

    def request_job(self, job_request):
        hash = hashlib.md5()

        hash.update(str(time.time()).encode('utf-8'))
        log_guid = hash.hexdigest()

        hash.update(str(time.time()).encode('utf-8'))
        job_name = hash.hexdigest()

        self.jobQueue.put(
            {
                str(self.pandaID):
                    {
                        u'jobsetID': self.jobsetId,
                        u'logGUID': log_guid,
                        u'cmtConfig': u'x86_64-centos7-gcc62-opt',
                        u'prodDBlocks': u'user.mlassnig:user.mlassnig.pilot.test.single.hits',
                        u'dispatchDBlockTokenForOut': u'NULL,NULL',
                        u'destinationDBlockToken': u'NULL,NULL',
                        u'destinationSE': self.get_panda_queue_name(),
                        u'realDatasets': job_name,
                        u'prodUserID': u'no_one',
                        u'GUID': self.guid,
                        u'realDatasetsIn': u'user.mlassnig:user.mlassnig.pilot.test.single.hits',
                        u'nSent': 0,
                        u'eventService': 'true',
                        u'cloud': u'US',
                        u'StatusCode': 0,
                        u'homepackage': u'AtlasOffline/22.0.5',
                        u'inFiles': self.inFiles,
                        u'processingType': u'pilot-ptest',
                        u'ddmEndPointOut': u'UTA_SWT2_DATADISK,UTA_SWT2_DATADISK',
                        u'fsize': u'118612262',
                        u'fileDestinationSE': f"{self.get_panda_queue_name()},{self.get_panda_queue_name()}",
                        u'scopeOut': u'panda',
                        u'minRamCount': 0,
                        u'jobDefinitionID': 7932,
                        u'maxWalltime': u'NULL',
                        u'scopeLog': u'panda',
                        u'transformation': u'Sim_tf.py',
                        u'maxDiskCount': 0,
                        u'coreCount': self.ncores,
                        u'prodDBlockToken': u'NULL',
                        u'transferType': u'NULL',
                        u'destinationDblock': job_name,
                        u'dispatchDBlockToken': u'NULL',
                        u'jobPars': (
                            '--eventService=True --skipEvents=0 --firstEvent=1 --preExec \'from AthenaCommon.DetFlags '
                            'import DetFlags;DetFlags.ID_setOn();DetFlags.Calo_setOff();'
                            'DetFlags.Muon_setOff();DetFlags.Lucid_setOff();DetFlags.Truth_setOff()\' '
                            '--athenaopts=--preloadlib=${ATLASMKLLIBDIR_PRELOAD}/libimf.so '
                            '--preInclude sim:SimulationJobOptions/preInclude.FrozenShowersFCalOnly.py,SimulationJobOptions/preInclude.BeamPipeKill.py '
                            '--geometryVersion ATLAS-R2-2016-01-00-00_VALIDATION --physicsList QGSP_BERT --randomSeed 1234 --conditionsTag OFLCOND-MC12-SIM-00 '
                            '--maxEvents=-1 --inputEvgenFile %s --outputHitsFile HITS_%s.pool.root' % (self.inFiles, job_name)),
                        u'attemptNr': 0,
                        u'swRelease': u'Atlas-22.0.5',
                        u'nucleus': u'NULL',
                        u'maxCpuCount': 0,
                        u'outFiles': u'HITS_%s.pool.root,%s.job.log.tgz' % (job_name, job_name),
                        u'currentPriority': 1000,
                        u'scopeIn': self.scope,
                        u'PandaID': self.pandaID,
                        u'sourceSite': u'NULL',
                        u'dispatchDblock': u'NULL',
                        u'prodSourceLabel': u'ptest',
                        u'checksum': u'ad:5d000974',
                        u'jobName': job_name,
                        u'ddmEndPointIn': u'UTA_SWT2_DATADISK',
                        u'taskID': self.taskId,
                        u'logFile': u'%s.job.log.tgz' % job_name
                }
            }
        )