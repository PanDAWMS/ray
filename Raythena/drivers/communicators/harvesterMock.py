import time
import hashlib
import os
from .baseCommunicator import BaseCommunicator


class HarvesterMock(BaseCommunicator):

    def __init__(self, config):
        super().__init__(config)
        self.event_ranges = None

    def get_event_ranges(self, evnt_request):

        if self.event_ranges is None:
            self.event_ranges = dict()
            for i in range(10000):
                rangeId = f"Range-{i+1:05}"
                self.event_ranges[rangeId] = {
                    'lastEvent': i + 1,
                    'eventRangeID': rangeId,
                    'startEvent': i + 1,
                    'PFN': os.path.join(os.getcwd(), "EVNT.01469903._009502.pool.root.1"),
                    'GUID': '9C81A8C7-FA15-D940-942B-2E40AF22C4D6'}
        return self.event_ranges

    def update_job(self, job_status):
        raise NotImplementedError("Base method not implemented")

    def update_events(self, evnt_status):
        raise NotImplementedError("Base method not i    mplemented")

    def get_panda_queue_name(self):
        return "NERSC_Cori_p2_ES"

    def get_job(self, job_request):
        hash = hashlib.md5()

        hash.update(str(time.time()).encode('utf-8'))
        log_guid = hash.hexdigest()

        guid = '9C81A8C7-FA15-D940-942B-2E40AF22C4D6'

        hash.update(str(time.time()).encode('utf-8'))
        job_name = hash.hexdigest()

        return {u'jobsetID': u'NULL',
                u'logGUID': log_guid,
                u'cmtConfig': u'x86_64-centos7-gcc8-opt',
                u'prodDBlocks': u'user.mlassnig:user.mlassnig.pilot.test.single.hits',
                u'dispatchDBlockTokenForOut': u'NULL,NULL',
                u'destinationDBlockToken': u'NULL,NULL',
                u'destinationSE': u'AGLT2_TEST',
                u'realDatasets': job_name,
                u'prodUserID': u'no_one',
                u'GUID': guid,
                u'realDatasetsIn': u'user.mlassnig:user.mlassnig.pilot.test.single.hits',
                u'nSent': 0,
                u'eventService': 'false',
                u'cloud': u'US',
                u'StatusCode': 0,
                u'homepackage': u'Athena/22.0.6',
                u'inFiles': u'EVNT.01469903._009502.pool.root.1',
                u'processingType': u'pilot-ptest',
                u'ddmEndPointOut': u'UTA_SWT2_DATADISK,UTA_SWT2_DATADISK',
                u'fsize': u'118612262',
                u'fileDestinationSE': u'AGLT2_TEST,AGLT2_TEST',
                u'scopeOut': u'panda',
                u'minRamCount': 0,
                u'jobDefinitionID': 7932,
                u'maxWalltime': u'NULL',
                u'scopeLog': u'panda',
                u'transformation': u'AtlasG4_tf.py',
                u'maxDiskCount': 0,
                u'coreCount': 32,
                u'prodDBlockToken': u'NULL',
                u'transferType': u'NULL',
                u'destinationDblock': job_name,
                u'dispatchDBlockToken': u'NULL',
                u'jobPars': u'--preExec "from AthenaCommon.DetFlags import DetFlags;DetFlags.ID_setOn();DetFlags.Calo_setOff();DetFlags.Muon_setOff();DetFlags.Lucid_setOff();DetFlags.Truth_setOff()" --athenaopts=--preloadlib=${ATLASMKLLIBDIR_PRELOAD}/libimf.so --preInclude sim:SimulationJobOptions/preInclude.FrozenShowersFCalOnly.py,SimulationJobOptions/preInclude.BeamPipeKill.py --geometryVersion ATLAS-R2-2016-01-00-00_VALIDATION --physicsList QGSP_BERT --randomSeed 1234 --conditionsTag OFLCOND-MC12-SIM-00 --maxEvents=1 --inputEvgenFile EVNT.01469903._009502.pool.root.1 --outputHitsFile HITS_%s.root' % job_name,
                u'attemptNr': 0,
                u'swRelease': u'Atlas-22.0.6',
                u'nucleus': u'NULL',
                u'maxCpuCount': 0,
                u'outFiles': u'HITS_%s.root,%s.job.log.tgz' % (job_name, job_name),
                u'currentPriority': 1000,
                u'scopeIn': u'mc15_13TeV',
                u'PandaID': '0',
                u'sourceSite': u'NULL',
                u'dispatchDblock': u'NULL',
                u'prodSourceLabel': u'ptest',
                u'checksum': u'ad:5d000974',
                u'jobName': job_name,
                u'ddmEndPointIn': u'UTA_SWT2_DATADISK',
                u'taskID': u'NULL',
                u'logFile': u'%s.job.log.tgz' % job_name}
