import json
from typing import Union, Tuple, Dict, List


# Messages sent by ray actor to the driver
class Messages:
    REQUEST_NEW_JOB = 0
    REQUEST_EVENT_RANGES = 1
    UPDATE_JOB = 2
    UPDATE_EVENT_RANGES = 3
    REQUEST_STATUS = 4
    PROCESS_DONE = 5
    IDLE = 6
    #
    REPLY_OK = 0
    REPLY_NO_MORE_EVENT_RANGES = 1
    REPLY_NO_MORE_JOBS = 2


class ESEncoder(json.JSONEncoder):

    def default(self, o: object) -> dict:

        if isinstance(o, PandaJobQueue):
            return o.jobs
        if isinstance(o, EventRangeQueue):
            return o.event_ranges_by_id

        if isinstance(o, PandaJobUpdate):
            return o.to_dict()
        if isinstance(o, EventRangeUpdate):
            return o.range_update

        if isinstance(o, PandaJobRequest):
            return o.to_dict()
        if isinstance(o, EventRangeRequest):
            return o.request

        if isinstance(o, PandaJob):
            return o.job
        if isinstance(o, EventRange):
            return o.to_dict()

        return super().default(o)


class PandaJobQueue:
    """
    Build from the reply to a job request. Harvester will provide the following JSON as a reply:

    {
        "pandaID": <PandaJob>,
        ...
    }
    """

    def __init__(self, jobs: dict = None) -> None:
        self.jobs = dict()
        self.distributed_jobs_ids = list()

        if jobs:
            self.add_jobs(jobs)

    def __getitem__(self, k):
        return self.jobs[k]

    def __setitem__(self, k: str, v: 'PandaJob') -> None:
        if isinstance(v, PandaJob):
            self.jobs[k] = v
        else:
            raise Exception(f"{v} is not of type {PandaJob}")

    def __iter__(self):
        return iter(self.jobs)

    def __len__(self) -> int:
        return len(self.jobs)

    def __contains__(self, k: str) -> bool:
        return self.has_job(k)

    def next_job_to_process(self) -> Union['PandaJob', None]:
        job_id, ranges_avail = self.next_job_id_to_process()

        if job_id is None:
            return None
        return self.jobs[job_id]

    def next_job_id_to_process(self) -> Tuple[Union[str, None], int]:
        """
        Return the jobid that workers requesting new jobs should work on which is defined by the job having the most events available.
        """
        max_job_id = None
        max_avail = 0

        if len(self.jobs) == 0:
            return max_job_id, max_avail

        for jobID, job in self.jobs.items():
            if ('eventService' not in job or job['eventService'].lower() == "false") and jobID not in self.distributed_jobs_ids:
                self.distributed_jobs_ids.append(jobID)
                return jobID, 0
            if job.nranges_available() > max_avail:
                max_avail = job.nranges_available()
                max_job_id = jobID
        return max_job_id, max_avail

    def has_job(self, panda_id: str) -> bool:
        return panda_id in self.jobs

    def add_jobs(self, jobs: Dict[str, dict]) -> None:
        for jobID, jobDef in jobs.items():
            self.jobs[jobID] = PandaJob(jobDef)

    def get_event_ranges(self, panda_id: str) -> 'EventRangeQueue':
        if panda_id in self.jobs:
            return self[panda_id].event_ranges_queue

    def process_event_ranges_update(self, ranges_update: 'EventRangeUpdate') -> None:
        for pandaID in ranges_update:
            self.get_event_ranges(pandaID).update_ranges(ranges_update[pandaID])

    def process_event_ranges_reply(self, reply: Dict[str, List[Dict]]) -> None:
        """
        Process an event ranges reply from harvester by adding ranges to each corresponding job already present in the queue
        """
        for pandaID, ranges in reply.items():
            if pandaID not in self.jobs:
                continue
            if not ranges:
                self.get_event_ranges(pandaID).no_more_ranges = True
            else:
                ranges_obj = list()
                for range_dict in ranges:
                    ranges_obj.append(EventRange.build_from_dict(range_dict))
                self.get_event_ranges(pandaID).concat(ranges_obj)

    @staticmethod
    def build_from_dict(jobs_dict: dict) -> 'PandaJobQueue':
        res = PandaJobQueue()
        res.add_jobs(jobs_dict)
        return res


class EventRangeQueue:
    """
    Each PandaJob has an eventRangeQueue that should be filled from a reply to an event ranges request:

    Harvester will reply using the following JSON schema which should be added to the PandaJobQueue / EventRangeQueue:
    {
        "pandaID": [
            {
                "eventRangeID": _,
                "LFN": _,
                "lastEvent": _,
                "startEvent": _,
                "scope": _,
                "GUID": _
            },
            ....
        ],
        ...
    }
    """

    def __init__(self) -> None:
        self.event_ranges_by_id = dict()
        self.rangesID_by_state = dict()
        self._no_more_ranges = False
        for state in EventRange.STATES:
            self.rangesID_by_state[state] = list()

    @property
    def no_more_ranges(self) -> bool:
        return self._no_more_ranges

    @no_more_ranges.setter
    def no_more_ranges(self, v: bool) -> None:
        self._no_more_ranges = v

    def __iter__(self):
        return iter(self.event_ranges_by_id)

    def __len__(self) -> int:
        return len(self.event_ranges_by_id)

    def __getitem__(self, k: str) -> 'EventRange':
        return self.event_ranges_by_id[k]

    def __setitem__(self, k: str, v: 'EventRange') -> None:
        if not isinstance(v, EventRange):
            raise Exception(f"{v} should be of type {EventRange}")
        self.event_ranges_by_id[k] = v

    def __contains__(self, k: str) -> bool:
        return k in self.event_ranges_by_id

    @staticmethod
    def build_from_list(ranges_list: list) -> 'EventRangeQueue':
        ranges_queue = EventRangeQueue()
        for r in ranges_list:
            ranges_queue.append(EventRange.build_from_dict(r))
        return ranges_queue

    def update_range_state(self, range_id: str, new_state: int) -> None:
        if range_id not in self.event_ranges_by_id:
            raise Exception(f"Trying to update non-existing eventrange {range_id}")

        r = self.event_ranges_by_id[range_id]
        self.rangesID_by_state[r.status].remove(range_id)
        r.status = new_state
        self.rangesID_by_state[r.status].append(range_id)

    def update_ranges(self, ranges_update: List[Dict]) -> None:
        for r in ranges_update:
            range_id = r['eventRangeID']
            range_status = r['eventStatus']
            if (range_status == "finished" or range_status == "failed" or range_status == "running")\
               and range_id not in self.rangesID_by_state[EventRange.ASSIGNED]:
                raise Exception(f"Unexpected state: tried to update unassigned {range_id} to {range_status}")
            if range_status == "finished":
                self.update_range_state(range_id, EventRange.DONE)
            else:
                self.update_range_state(range_id, EventRange.FAILED)

    def nranges_remaining(self) -> int:
        return len(self.event_ranges_by_id) - (self.nranges_done() + self.nranges_failed())

    def nranges_available(self) -> int:
        return len(self.rangesID_by_state[EventRange.READY])

    def nranges_assigned(self) -> int:
        return len(self.rangesID_by_state[EventRange.ASSIGNED])

    def nranges_failed(self) -> int:
        return len(self.rangesID_by_state[EventRange.FAILED])

    def nranges_done(self) -> int:
        return len(self.rangesID_by_state[EventRange.DONE])

    def append(self, event_range: Union[dict, 'EventRange']) -> None:
        if isinstance(event_range, dict):
            event_range = EventRange.build_from_dict(event_range)
        self.event_ranges_by_id[event_range.eventRangeID] = event_range
        self.rangesID_by_state[event_range.status].append(event_range.eventRangeID)

    def concat(self, ranges: List[Union[dict, 'EventRange']]) -> None:
        for r in ranges:
            self.append(r)

    def get_next_ranges(self, nranges: int) -> List['EventRange']:
        res = list()
        nranges = min(nranges, len(self.rangesID_by_state[EventRange.READY]))
        for i in range(nranges):
            rangeID = self.rangesID_by_state[EventRange.READY].pop()
            r = self.event_ranges_by_id[rangeID]
            r.status = EventRange.ASSIGNED
            self.rangesID_by_state[EventRange.ASSIGNED].append(rangeID)
            res.append(r)

        return res


class PandaJobUpdate:
    """
    Pilot 2 sends the following job update:

    {
        'node': ['nid00038'],
        'startTime': ['1574112042.86'],
        'jobMetrics': ['coreCount=32'],
        'siteName': ['NERSC_Cori_p2_ES'],
        'timestamp': ['2019-11-18T13:20:45-08:00'],
        'coreCount': ['32'],
        'attemptNr': ['0'],
        'jobId': ['7a75654803d17d54f9129e2a6974beda'],
        'batchID': ['25932742'],
        'state': ['starting'],
        'schedulerID': ['unknown'],
        'pilotID': ['unknown|SLURM|PR|2.2.2 (1)']
    }

    """

    def __init__(self, **kwargs) -> None:
        for k, v in kwargs.items():
            setattr(self, k, v)

    def __str__(self) -> str:
        return str(self.__dict__)

    def to_dict(self) -> dict:
        return self.__dict__


class EventRangeUpdate:
    """
    Event ranges update sent by pilot 2 using JSON schema:
    [
        {
            "zipFile":
            {
                "numEvents": 2,
                "lfn": "EventService_premerge_Range-00007.tar",
                "adler32": "36503831",
                "objstoreID": 1641,
                "fsize": 860160,
                "pathConvention": 1000
            },
            "eventRanges": [
                {
                    "eventRangeID": "Range-00007",
                    "eventStatus": "finished"
                },
                {
                    "eventRangeID": "Range-00009",
                    "eventStatus": "finished"
                }
            ]
        }
    ]

    If no file is produced by the range update (e.g. failed events), the following schema is sent:

    [
        {
            "errorCode": 1220,
            "eventRangeID": "Range-00003",
            "eventStatus": "failed"
        }
    ]

    The JSON schema that should be sent to harvester is as shown below.

    eventstatus in [running, finished, failed, fatal]
    type in [output, es_output, zip_output, log]

    If it is an event upate, only eventRangeID and eventStatus fields are required.
    If output files are produced path, type should be specified

    {
        "pandaID": [
            {
                "eventRangeID": Range-00007,
                "eventStatus: finished,
                "path": EventService_premerge_Range-00007.tar,
                "type": zip_output,
                "chksum" 36503831,
                "fsize": 860160,
                "guid": None
            },
            {
                "eventRangeID": Range-00009,
                "eventStatus: finished,
                "path": EventService_premerge_Range-00007.tar,
                "type": zip_output,
                "chksum" 36503831,
                "fsize": 860160,
                "guid": None
            }
        ],
        ...
    }

    """

    def __init__(self, range_update: Dict[str, List[dict]]) -> None:
        for v in range_update.values():
            if not isinstance(v, list):
                raise Exception(f"Expecting type list for element {v}")
        self.range_update = range_update

    def __len__(self) -> int:
        return len(self.range_update)

    def __iter__(self):
        return iter(self.range_update)

    def __str__(self) -> str:
        return json.dumps(self.range_update)

    def __getitem__(self, k: str) -> list:
        return self.range_update[k]

    def __setitem__(self, k: str, v: list) -> None:
        if not isinstance(v, list):
            raise Exception(f"Expecting type list for element {v}")
        self.range_update[k] = v

    @staticmethod
    def build_from_dict(panda_id: str, range_update: dict) -> 'EventRangeUpdate':

        update_dict = dict()
        update_dict[panda_id] = list()

        if isinstance(range_update, dict) and "zipFile" not in range_update and "eventRangeID" not in range_update:
            range_update = json.loads(range_update['eventRanges'][0])

        for range_elt in range_update:
            file_info = range_elt.get('zipFile', None)
            ranges_info = range_elt.get('eventRanges', None)
            file_data = dict()

            if file_info:
                if file_info['lfn'].find('.root.Range') > -1:
                    ftype = "es_output"
                else:
                    ftype = "zip_output"
                file_data['path'] = file_info['lfn']
                file_data['type'] = ftype
                file_data['chksum'] = file_info['adler32']
                file_data['fsize'] = file_info['fsize']
                file_data['guid'] = None

            if ranges_info:
                for rangeInfo in ranges_info:
                    elt = dict()
                    elt['eventRangeID'] = rangeInfo['eventRangeID']
                    elt['eventStatus'] = rangeInfo['eventStatus']
                    elt.update(file_data)
                    update_dict[panda_id].append(elt)
            else:
                elt = dict()
                elt['eventRangeID'] = range_elt['eventRangeID']
                elt['eventStatus'] = range_elt['eventStatus']
                elt.update(file_data)
                update_dict[panda_id].append(elt)

        return EventRangeUpdate(update_dict)


class PandaJobRequest:
    """
    Pilot2 requests job using the following JSON schema:
    {
        "node": _,
        "diskSpace": _,
        "workingGroup": _,
        "prodSourceLabel": _,
        "computingElement": _,
        "siteName": _,
        "resourceType": _,
        "mem": _,
        "cpu": _,
        "allowOtherCountry": _
    }

    Note that harvester will ignore the content of the job request file and simply check if it exists
    """
    def __init__(self, node: str = None, disk_space: str = None, working_group: str = None, prod_source_label: str = None,
                 computing_element: str = None, site_name: str = None, resource_type: str = None, mem: str = None,
                 cpu: str = None, allow_other_country: str = None) -> None:
        self.node = node
        self.diskSpace = disk_space
        self.workingGroup = working_group
        self.prodSourceLabel = prod_source_label
        self.computingElement = computing_element
        self.siteName = site_name
        self.resourceType = resource_type
        self.mem = mem
        self.cpu = cpu
        self.allowOtherCountry = allow_other_country

    def __str__(self) -> str:
        return str(self.__dict__)

    def to_dict(self) -> dict:
        return self.__dict__


class EventRangeRequest:
    """
    Send event request to harvester. JSON schema:
    {
        "pandaID": {
            "nRanges": _,
            "pandaID": _,
            "taskID": _,
            "jobsetID": _
        },
        ...
    }
    """
    def __init__(self) -> None:
        self.request = dict()

    def __len__(self) -> int:
        return len(self.request)

    def __iter__(self):
        return iter(self.request)

    def __getitem__(self, k: str) -> dict:
        return self.request[k]

    def __str__(self) -> dict:
        return json.dumps(self.request)

    def add_event_request(self, panda_id, n_ranges, task_id, jobset_id) -> None:
        self.request[panda_id] = {'pandaID': panda_id, 'nRanges': n_ranges, 'taskID': task_id, 'jobsetID': jobset_id}

    @staticmethod
    def build_from_dict(request_dict: dict) -> 'EventRangeRequest':
        request = EventRangeRequest()
        request.request.update(request_dict)
        return request


class PandaJob:
    """
    Wrapper for a panda jobspec. Usually contains the following fields:
    {
        'jobsetID': self.jobsetId,
        'logGUID': log_guid,
        'cmtConfig': 'x86_64-centos7-gcc8-opt',
        'prodDBlocks': 'user.mlassnig:user.mlassnig.pilot.test.single.hits',
        'dispatchDBlockTokenForOut': 'NULL,NULL',
        'destinationDBlockToken': 'NULL,NULL',
        'destinationSE': self.get_panda_queue_name(),
        'realDatasets': job_name,
        'prodUserID': 'no_one',
        'GUID': self.guid,
        'realDatasetsIn': 'user.mlassnig:user.mlassnig.pilot.test.single.hits',
        'nSent': 0,
        'eventService': 'true',
        'cloud': 'US',
        'StatusCode': 0,
        'homepackage': 'Athena/22.0.8',
        'inFiles': self.inFile,
        'processingType': 'pilot-ptest',
        'ddmEndPointOut': 'UTA_SWT2_DATADISK,UTA_SWT2_DATADISK',
        'fsize': '118612262',
        'fileDestinationSE': f"{self.get_panda_queue_name()},{self.get_panda_queue_name()}",
        'scopeOut': 'panda',
        'minRamCount': 0,
        'jobDefinitionID': 7932,
        'maxWalltime': 'NULL',
        'scopeLog': 'panda',
        'transformation': 'AtlasG4_tf.py',
        'maxDiskCount': 0,
        'coreCount': self.ncores,
        'prodDBlockToken': 'NULL',
        'transferType': 'NULL',
        'destinationDblock': job_name,
        'dispatchDBlockToken': 'NULL',
        'jobPars': ' --multiprocess --eventService=True --skipEvents=0 --firstEvent=1
        --preExec "from AthenaCommon.DetFlags import DetFlags;DetFlags.ID_setOn();DetFlags.Calo_setOff();
        DetFlags.Muon_setOff();DetFlags.Lucid_setOff();DetFlags.Truth_setOff()"
        --athenaopts=--preloadlib=${ATLASMKLLIBDIR_PRELOAD}/libimf.so
        --preInclude sim:SimulationJobOptions/preInclude.FrozenShowersFCalOnly.py,SimulationJobOptions/preInclude.BeamPipeKill.py
        --geometryVersion ATLAS-R2-2016-01-00-00_VALIDATION --physicsList QGSP_BERT --randomSeed 1234
        --conditionsTag OFLCOND-MC12-SIM-00 --maxEvents=-1 --inputEvgenFile EVNT.01469903._009502.pool.root.1
        --outputHitsFile HITS_%s.pool.root' % job_name,
        'attemptNr': 0,
        'swRelease': 'Atlas-22.0',
        'nucleus': 'NULL',
        'maxCpuCount': 0,
        'outFiles': 'HITS_%s.pool.root,%s.job.log.tgz' % (job_name, job_name),
        'currentPriority': 1000,
        'scopeIn': self.scope,
        'PandaID': self.pandaID,
        'sourceSite': 'NULL',
        'dispatchDblock': 'NULL',
        'prodSourceLabel': 'ptest',
        'checksum': 'ad:5d000974',
        'jobName': job_name,
        'ddmEndPointIn': 'UTA_SWT2_DATADISK',
        'taskID': self.taskId,
        'logFile': '%s.job.log.tgz' % job_name
    }
    """

    def __init__(self, job_def: dict) -> None:
        self.job = job_def
        if "PandaID" in self:
            self["PandaID"] = str(self["PandaID"])
        self.event_ranges_queue = EventRangeQueue()

    def nranges_available(self) -> int:
        return self.event_ranges_queue.nranges_available()

    def get_next_ranges(self, nranges) -> List['EventRange']:
        return self.event_ranges_queue.get_next_ranges(nranges)

    def get_pandaQueue(self) -> str:
        return self['destinationSE']

    def get_id(self) -> str:
        return self['PandaID']

    def __str__(self) -> str:
        return json.dumps(self.job)

    def __getitem__(self, k: str) -> str:
        return self.job[k]

    def __setitem__(self, k: str, v: str) -> str:
        self.job[k] = v

    def __len__(self) -> int:
        return len(self.job)

    def __iter__(self):
        return iter(self.job)

    def __contains__(self, k: str) -> bool:
        return k in self.job


class EventRange:
    """
    Hold an event range:
    {
        "eventRangeID": _,
        "PFN": _,
        "lastEvent": _,
        "startEvent": _,
        "GUID": _
    }

    Note that while harvester returns a LFN field, Athena needs PFN so the value fetched from LFN is assigned to PFN
    """

    READY = 0
    ASSIGNED = 1
    DONE = 2
    FAILED = 3
    STATES = [READY, ASSIGNED, DONE, FAILED]

    def __init__(self, event_range_id: str, start_event: int, last_event: int, pfn: str, guid: str, scope: str) -> None:
        self.lastEvent = last_event
        self.eventRangeID = event_range_id
        self.startEvent = start_event
        self.PFN = pfn
        self.GUID = guid
        self.scope = scope
        self.status = EventRange.READY
        self.retry = 0

    def set_assigned(self) -> None:
        self.status = EventRange.ASSIGNED

    def set_done(self) -> None:
        self.status = EventRange.DONE

    def set_failed(self) -> None:
        self.status = EventRange.FAILED

    def nevents(self) -> int:
        return self.lastEvent - self.startEvent + 1

    def __str__(self) -> str:
        return json.dumps(self.to_dict())

    def to_dict(self) -> dict:
        return {
            'PFN': self.PFN,
            'lastEvent': self.lastEvent,
            'eventRangeID': self.eventRangeID,
            'startEvent': self.startEvent,
            'GUID': self.GUID
        }

    @staticmethod
    def build_from_dict(event_ranges_dict: dict) -> 'EventRange':
        return EventRange(event_ranges_dict['eventRangeID'],
                          event_ranges_dict['startEvent'],
                          event_ranges_dict['lastEvent'],
                          event_ranges_dict.get('PFN', event_ranges_dict.get('LFN', None)),
                          event_ranges_dict['GUID'],
                          event_ranges_dict['scope'])
