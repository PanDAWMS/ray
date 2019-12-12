import pytest
import hashlib
import time
from Raythena.utils.ray import setup_ray, shutdown_ray
from Raythena.utils.config import Config


@pytest.fixture(scope="session")
def config_path():
    return "tests/testconf.yaml"


@pytest.fixture(scope="class")
def requires_ray(config):
    setup_ray(config)
    yield
    shutdown_ray(config)


@pytest.fixture(scope="class")
def config(config_path):
    return Config(
        config_path, config=None, debug=False, payload_bindir=None,
        ray_driver=None, ray_head_ip=None, ray_redis_password=None, ray_redis_port=None,
        ray_workdir=None, harvester_endpoint=None, panda_queue=None, core_per_node=None)


@pytest.fixture
def nevents():
    return 100


@pytest.fixture
def njobs():
    return 3


@pytest.fixture(params=[True, False])
def is_eventservice(request):
    return request.param


@pytest.fixture
def pandaids(njobs):
    res = []
    for i in range(njobs):
        hash = hashlib.md5()
        hash.update(str(time.time()).encode('utf-8'))
        res.append(hash.hexdigest())
    return res


@pytest.fixture
def sample_ranges(nevents, pandaids):
    res = {}
    for pandaID in pandaids:
        range_list = list()
        res[pandaID] = range_list
        for i in range(nevents):
            range_list.append({
                'lastEvent': i,
                'eventRangeID': f"Range-{i:05}",
                'startEvent': i,
                'scope': '13Mev',
                'LFN': "EVNT-2.pool.root.1",
                'GUID': '0'})
    return res


@pytest.fixture
def sample_rangeupdate(nevents):
    return [
        {
            "zipFile":
            {
                "numEvents": nevents,
                "lfn": "EventService_premerge_Range-00000.tar",
                "adler32": "36503831",
                "objstoreID": 1641,
                "fsize": 860160,
                "pathConvention": 1000
            },
            "eventRanges": [{"eventRangeID": f"Range-{i:05}", "eventStatus": "finished"} for i in range(nevents)]
        }
    ]


@pytest.fixture
def sample_failed_rangeupdate(nevents):
    return [{"eventRangeID": f"Range-{i:05}", "eventStatus": "failed"} for i in range(nevents)]


@pytest.fixture
def sample_multijobs(request, is_eventservice, pandaids):
    res = {}
    for pandaID in pandaids:
        hash = hashlib.md5()

        hash.update(str(time.time()).encode('utf-8'))
        log_guid = hash.hexdigest()

        hash.update(str(time.time()).encode('utf-8'))
        job_name = hash.hexdigest()

        jobsetId = '0'
        taskId = '0'
        ncores = '8'
        guid = '0'
        scope = "13Mev"
        panda_queue_name = f"pandaqueue_{hash.hexdigest()}"
        inFiles = "EVNT-2.pool.root.1"
        res[pandaID] = {
            u'jobsetID': jobsetId,
            u'logGUID': log_guid,
            u'cmtConfig': u'x86_64-slc6-gcc49-opt',
            u'prodDBlocks': u'user.mlassnig:user.mlassnig.pilot.test.single.hits',
            u'dispatchDBlockTokenForOut': u'NULL,NULL',
            u'destinationDBlockToken': u'NULL,NULL',
            u'destinationSE': panda_queue_name,
            u'realDatasets': job_name,
            u'prodUserID': u'no_one',
            u'GUID': guid,
            u'realDatasetsIn': u'user.mlassnig:user.mlassnig.pilot.test.single.hits',
            u'nSent': 0,
            u'eventService': str(is_eventservice),
            u'cloud': u'US',
            u'StatusCode': 0,
            u'homepackage': u'AtlasOffline/21.0.15',
            u'inFiles': inFiles,
            u'processingType': u'pilot-ptest',
            u'ddmEndPointOut': u'UTA_SWT2_DATADISK,UTA_SWT2_DATADISK',
            u'fsize': u'118612262',
            u'fileDestinationSE': f"{panda_queue_name},{panda_queue_name}",
            u'scopeOut': u'panda',
            u'minRamCount': 0,
            u'jobDefinitionID': 7932,
            u'maxWalltime': u'NULL',
            u'scopeLog': u'panda',
            u'transformation': u'Sim_tf.py',
            u'maxDiskCount': 0,
            u'coreCount': ncores,
            u'prodDBlockToken': u'NULL',
            u'transferType': u'NULL',
            u'destinationDblock': job_name,
            u'dispatchDBlockToken': u'NULL',
            u'jobPars': (
                '--eventService=%s --skipEvents=0 --firstEvent=1 --preExec "from AthenaCommon.DetFlags '
                'import DetFlags;DetFlags.ID_setOn();DetFlags.Calo_setOff();'
                'DetFlags.Muon_setOff();DetFlags.Lucid_setOff();DetFlags.Truth_setOff() "'
                '--athenaopts=--preloadlib=${ATLASMKLLIBDIR_PRELOAD}/libimf.so '
                '--preInclude sim:SimulationJobOptions/preInclude.FrozenShowersFCalOnly.py,SimulationJobOptions/preInclude.BeamPipeKill.py '
                '--geometryVersion ATLAS-R2-2016-01-00-00_VALIDATION --physicsList QGSP_BERT --randomSeed 1234 --conditionsTag OFLCOND-MC12-SIM-00 '
                '--maxEvents=-1 --inputEvgenFile %s --outputHitsFile HITS_%s.pool.root)' % (str(is_eventservice), inFiles, job_name)),
            u'attemptNr': 0,
            u'swRelease': u'Atlas-21.0.15',
            u'nucleus': u'NULL',
            u'maxCpuCount': 0,
            u'outFiles': u'HITS_%s.pool.root,%s.job.log.tgz' % (job_name, job_name),
            u'currentPriority': 1000,
            u'scopeIn': scope,
            u'PandaID': pandaID,
            u'sourceSite': u'NULL',
            u'dispatchDblock': u'NULL',
            u'prodSourceLabel': u'ptest',
            u'checksum': u'ad:5d000974',
            u'jobName': job_name,
            u'ddmEndPointIn': u'UTA_SWT2_DATADISK',
            u'taskID': taskId,
            u'logFile': u'%s.job.log.tgz' % job_name
        }
    return res


@pytest.fixture
def sample_job(is_eventservice):
    hash = hashlib.md5()

    hash.update(str(time.time()).encode('utf-8'))
    log_guid = hash.hexdigest()

    hash.update(str(time.time()).encode('utf-8'))
    job_name = hash.hexdigest()
    pandaID = '0'
    jobsetId = '0'
    taskId = '0'
    ncores = '8'
    guid = '0'
    scope = "13Mev"
    panda_queue_name = "pandaqueue"
    inFiles = "EVNT-2.pool.root.1"
    return {
        pandaID:
            {
                u'jobsetID': jobsetId,
                u'logGUID': log_guid,
                u'cmtConfig': u'x86_64-slc6-gcc49-opt',
                u'prodDBlocks': u'user.mlassnig:user.mlassnig.pilot.test.single.hits',
                u'dispatchDBlockTokenForOut': u'NULL,NULL',
                u'destinationDBlockToken': u'NULL,NULL',
                u'destinationSE': panda_queue_name,
                u'realDatasets': job_name,
                u'prodUserID': u'no_one',
                u'GUID': guid,
                u'realDatasetsIn': u'user.mlassnig:user.mlassnig.pilot.test.single.hits',
                u'nSent': 0,
                u'eventService': str(is_eventservice),
                u'cloud': u'US',
                u'StatusCode': 0,
                u'homepackage': u'AtlasOffline/21.0.15',
                u'inFiles': inFiles,
                u'processingType': u'pilot-ptest',
                u'ddmEndPointOut': u'UTA_SWT2_DATADISK,UTA_SWT2_DATADISK',
                u'fsize': u'118612262',
                u'fileDestinationSE': f"{panda_queue_name},{panda_queue_name}",
                u'scopeOut': u'panda',
                u'minRamCount': 0,
                u'jobDefinitionID': 7932,
                u'maxWalltime': u'NULL',
                u'scopeLog': u'panda',
                u'transformation': u'Sim_tf.py',
                u'maxDiskCount': 0,
                u'coreCount': ncores,
                u'prodDBlockToken': u'NULL',
                u'transferType': u'NULL',
                u'destinationDblock': job_name,
                u'dispatchDBlockToken': u'NULL',
                u'jobPars': (
                    '--eventService=%s --skipEvents=0 --firstEvent=1 --preExec "from AthenaCommon.DetFlags '
                    'import DetFlags;DetFlags.ID_setOn();DetFlags.Calo_setOff();'
                    'DetFlags.Muon_setOff();DetFlags.Lucid_setOff();DetFlags.Truth_setOff() "'
                    '--athenaopts=--preloadlib=${ATLASMKLLIBDIR_PRELOAD}/libimf.so '
                    '--preInclude sim:SimulationJobOptions/preInclude.FrozenShowersFCalOnly.py,SimulationJobOptions/preInclude.BeamPipeKill.py '
                    '--geometryVersion ATLAS-R2-2016-01-00-00_VALIDATION --physicsList QGSP_BERT --randomSeed 1234 --conditionsTag OFLCOND-MC12-SIM-00 '
                    '--maxEvents=-1 --inputEvgenFile %s --outputHitsFile HITS_%s.pool.root)' % (str(is_eventservice), inFiles, job_name)),
                u'attemptNr': 0,
                u'swRelease': u'Atlas-21.0.15',
                u'nucleus': u'NULL',
                u'maxCpuCount': 0,
                u'outFiles': u'HITS_%s.pool.root,%s.job.log.tgz' % (job_name, job_name),
                u'currentPriority': 1000,
                u'scopeIn': scope,
                u'PandaID': pandaID,
                u'sourceSite': u'NULL',
                u'dispatchDblock': u'NULL',
                u'prodSourceLabel': u'ptest',
                u'checksum': u'ad:5d000974',
                u'jobName': job_name,
                u'ddmEndPointIn': u'UTA_SWT2_DATADISK',
                u'taskID': taskId,
                u'logFile': u'%s.job.log.tgz' % job_name
            }
    }
