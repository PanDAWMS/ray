import hashlib
import time

import pytest

from raythena.utils.config import Config
from raythena.utils.ray import setup_ray, shutdown_ray


@pytest.fixture(scope="session")
def config_path():
    return "tests/testconf.yaml"


@pytest.fixture(scope="class")
def requires_ray(config_base):
    setup_ray(config_base)
    yield
    shutdown_ray(config_base)


@pytest.fixture(scope="class")
def config_base(config_path):
    return Config(config_path,
                  config=None,
                  debug=False,
                  ray_head_ip=None,
                  ray_redis_password=None,
                  ray_redis_port=None,
                  ray_workdir=None,
                  harvester_endpoint=None,
                  panda_queue=None,
                  core_per_node=None)


@pytest.fixture
def config(config_base, tmp_path):
    config_base.ray["outputdir"] = tmp_path
    return config_base


@pytest.fixture(scope="session")
def nevents():
    return 16


@pytest.fixture
def njobs():
    return 1


@pytest.fixture
def is_eventservice(request):
    return True


@pytest.fixture
def pandaids(njobs):
    res = []
    for i in range(njobs):
        hash = hashlib.md5()
        hash.update(str(time.time()).encode('utf-8'))
        res.append(hash.hexdigest())
    return res


@pytest.fixture(scope="session")
def nfiles():
    return 4


@pytest.fixture
def nevents_per_file(nevents, nfiles):
    return nevents // nfiles


@pytest.fixture
def nhits_per_file(nevents_per_file):
    return nevents_per_file // 2

@pytest.fixture
def range_ids(nfiles, nevents_per_file):
    return [f"EVNT_{file}.pool.root.1-{event}" for event in range(1, nevents_per_file + 1) for file in range(nfiles)]

@pytest.fixture
def sample_ranges(nevents, pandaids, input_output_file_list):
    res = {}
    (input_files, _) = input_output_file_list
    nfiles = len(input_files)
    files = [f"/path/to/{i}" for i in input_files]
    for pandaID in pandaids:
        range_list = list()
        res[pandaID] = range_list
        for i in range(nevents):
            range_list.append({
                'lastEvent': i,
                'eventRangeID': f"Range-{i:05}",
                'startEvent': i,
                'scope': '13Mev',
                'LFN': files[i % nfiles],
                'GUID': '0'
            })
    return res


@pytest.fixture
def sample_rangeupdate(range_ids):
    return [{
        "zipFile": {
            "numEvents": len(range_ids),
            "lfn": "EventService_premerge_Range-00000.tar",
            "adler32": "36503831",
            "objstoreID": 1641,
            "fsize": 860160,
            "pathConvention": 1000
        },
        "eventRanges": [{
            "eventRangeID": r,
            "eventStatus": "finished"
        } for r in range_ids]
    }]


@pytest.fixture
def sample_failed_rangeupdate(range_ids):
    return [{
        "eventRangeID": r,
        "eventStatus": "failed"
    } for r in range_ids]


@pytest.fixture
def input_output_file_list(nfiles, nhits_per_file, nevents_per_file):
    if nhits_per_file > nevents_per_file:
        assert nhits_per_file % nevents_per_file == 0
        n = nhits_per_file // nevents_per_file
        n_output_files = nfiles // n
    else:
        assert nevents_per_file % nhits_per_file == 0
        n = nevents_per_file // nhits_per_file
        n_output_files = nfiles * n
    output_files = [f"HITS_{i}.pool.root.1" for i in range(n_output_files)]
    input_files = [f"EVNT_{i}.pool.root.1" for i in range(nfiles)]
    return (input_files, output_files)


@pytest.fixture
def sample_multijobs(request, input_output_file_list, is_eventservice, pandaids, nhits_per_file, nevents_per_file):
    res = {}
    (input_files, output_files) = input_output_file_list
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
        inFiles = ",".join(input_files)
        outFiles = ",".join(output_files)
        outFilesShort = f"[{','.join([str(i) for i in range(len(outFiles))])}]"
        res[pandaID] = {
            u'jobsetID':
                jobsetId,
            u'nEventsPerInputFile': nevents_per_file,
            u'esmergeSpec': {
                "transPath": "",
                "jobParameters": "",
                "nEventsPerOutputFile": nhits_per_file
            },
            u'logGUID':
                log_guid,
            u'cmtConfig':
                u'x86_64-slc6-gcc49-opt',
            u'prodDBlocks':
                u'user.mlassnig:user.mlassnig.pilot.test.single.hits',
            u'dispatchDBlockTokenForOut':
                u'NULL,NULL',
            u'destinationDBlockToken':
                u'NULL,NULL',
            u'destinationSE':
                panda_queue_name,
            u'realDatasets':
                job_name,
            u'prodUserID':
                u'no_one',
            u'GUID':
                ",".join([f"{guid}{i}" for i in range(len(input_files))]),
            u'realDatasetsIn':
                u'user.mlassnig:user.mlassnig.pilot.test.single.hits',
            u'nSent':
                0,
            u'eventService':
                str(is_eventservice),
            u'cloud':
                u'US',
            u'StatusCode':
                0,
            u'homepackage':
                u'AtlasOffline/21.0.15',
            u'inFiles':
                inFiles,
            u'processingType':
                u'pilot-ptest',
            u'ddmEndPointOut':
                u'UTA_SWT2_DATADISK,UTA_SWT2_DATADISK',
            u'fsize':
                u'118612262',
            u'fileDestinationSE':
                f"{panda_queue_name},{panda_queue_name}",
            u'scopeOut':
                u'panda',
            u'minRamCount':
                0,
            u'jobDefinitionID':
                7932,
            u'maxWalltime':
                u'NULL',
            u'scopeLog':
                u'panda',
            u'transformation':
                u'Sim_tf.py',
            u'maxDiskCount':
                0,
            u'coreCount':
                ncores,
            u'prodDBlockToken':
                u'NULL',
            u'transferType':
                u'NULL',
            u'destinationDblock':
                job_name,
            u'dispatchDBlockToken':
                u'NULL',
            u'jobPars': (
                '--eventService=%s --skipEvents=0 --firstEvent=1 --preExec "from AthenaCommon.DetFlags '
                'import DetFlags;DetFlags.ID_setOn();DetFlags.Calo_setOff();'
                'DetFlags.Muon_setOff();DetFlags.Lucid_setOff();DetFlags.Truth_setOff() "'
                '--athenaopts=--preloadlib=${ATLASMKLLIBDIR_PRELOAD}/libimf.so '
                '--preInclude sim:SimulationJobOptions/preInclude.FrozenShowersFCalOnly.py,SimulationJobOptions/preInclude.BeamPipeKill.py '
                '--geometryVersion ATLAS-R2-2016-01-00-00_VALIDATION --physicsList QGSP_BERT --randomSeed 1234 --conditionsTag OFLCOND-MC12-SIM-00 '
                '--maxEvents=-1 --inputEvgenFile %s --outputHitsFile HITS_%s.pool.root)'
                % (str(is_eventservice), inFiles, outFilesShort)),
            u'attemptNr':
                0,
            u'swRelease':
                u'Atlas-21.0.15',
            u'nucleus':
                u'NULL',
            u'maxCpuCount':
                0,
            u'outFiles':
                outFiles,
            u'currentPriority':
                1000,
            u'scopeIn':
                scope,
            u'PandaID':
                pandaID,
            u'sourceSite':
                u'NULL',
            u'dispatchDblock':
                u'NULL',
            u'prodSourceLabel':
                u'ptest',
            u'checksum':
                u'ad:5d000974',
            u'jobName':
                job_name,
            u'ddmEndPointIn':
                u'UTA_SWT2_DATADISK',
            u'taskID':
                taskId,
            u'logFile':
                u'%s.job.log.tgz' % job_name
        }
    return res


@pytest.fixture
def sample_job(is_eventservice, input_output_file_list, nhits_per_file, nevents_per_file):
    hash = hashlib.md5()
    (input_files, output_files) = input_output_file_list
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
    inFiles = ",".join(input_files)
    outFiles = ",".join(output_files)
    outFilesShort = f"[{','.join([str(i) for i in range(len(outFiles))])}]"
    return {
        pandaID: {
            u'jobsetID':
                jobsetId,
            u'logGUID':
                log_guid,
            u'nEventsPerInputFile': nevents_per_file,
            u'esmergeSpec': {
                "transPath": "",
                "jobParameters": "",
                "nEventsPerOutputFile": nhits_per_file
            },
            u'cmtConfig':
                u'x86_64-slc6-gcc49-opt',
            u'prodDBlocks':
                u'user.mlassnig:user.mlassnig.pilot.test.single.hits',
            u'dispatchDBlockTokenForOut':
                u'NULL,NULL',
            u'destinationDBlockToken':
                u'NULL,NULL',
            u'destinationSE':
                panda_queue_name,
            u'realDatasets':
                job_name,
            u'prodUserID':
                u'no_one',
            u'GUID':
                guid,
            u'realDatasetsIn':
                u'user.mlassnig:user.mlassnig.pilot.test.single.hits',
            u'nSent':
                0,
            u'eventService':
                str(is_eventservice),
            u'cloud':
                u'US',
            u'StatusCode':
                0,
            u'homepackage':
                u'AtlasOffline/21.0.15',
            u'inFiles':
                inFiles,
            u'processingType':
                u'pilot-ptest',
            u'ddmEndPointOut':
                u'UTA_SWT2_DATADISK,UTA_SWT2_DATADISK',
            u'fsize':
                u'118612262',
            u'fileDestinationSE':
                f"{panda_queue_name},{panda_queue_name}",
            u'scopeOut':
                u'panda',
            u'minRamCount':
                0,
            u'jobDefinitionID':
                7932,
            u'maxWalltime':
                u'NULL',
            u'scopeLog':
                u'panda',
            u'transformation':
                u'Sim_tf.py',
            u'maxDiskCount':
                0,
            u'coreCount':
                ncores,
            u'prodDBlockToken':
                u'NULL',
            u'transferType':
                u'NULL',
            u'destinationDblock':
                job_name,
            u'dispatchDBlockToken':
                u'NULL',
            u'jobPars': (
                '--eventService=%s --skipEvents=0 --firstEvent=1 --preExec "from AthenaCommon.DetFlags '
                'import DetFlags;DetFlags.ID_setOn();DetFlags.Calo_setOff();'
                'DetFlags.Muon_setOff();DetFlags.Lucid_setOff();DetFlags.Truth_setOff() "'
                '--athenaopts=--preloadlib=${ATLASMKLLIBDIR_PRELOAD}/libimf.so '
                '--preInclude sim:SimulationJobOptions/preInclude.FrozenShowersFCalOnly.py,SimulationJobOptions/preInclude.BeamPipeKill.py '
                '--geometryVersion ATLAS-R2-2016-01-00-00_VALIDATION --physicsList QGSP_BERT --randomSeed 1234 --conditionsTag OFLCOND-MC12-SIM-00 '
                '--maxEvents=-1 --inputEvgenFile %s --outputHitsFile HITS_%s.pool.root)'
                % (str(is_eventservice), inFiles, outFilesShort)),
            u'attemptNr':
                0,
            u'swRelease':
                u'Atlas-21.0.15',
            u'nucleus':
                u'NULL',
            u'maxCpuCount':
                0,
            u'outFiles':
                outFiles,
            u'currentPriority':
                1000,
            u'scopeIn':
                scope,
            u'PandaID':
                pandaID,
            u'sourceSite':
                u'NULL',
            u'dispatchDblock':
                u'NULL',
            u'prodSourceLabel':
                u'ptest',
            u'checksum':
                u'ad:5d000974',
            u'jobName':
                job_name,
            u'ddmEndPointIn':
                u'UTA_SWT2_DATADISK',
            u'taskID':
                taskId,
            u'logFile':
                u'%s.job.log.tgz' % job_name
        }
    }
