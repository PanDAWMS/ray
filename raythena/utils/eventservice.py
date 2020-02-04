import json
import os

from typing import Union, Tuple, Dict, List, Iterator


# Messages sent by ray actor to the driver
class Messages(object):
    """
    Defines messages exchanged between ray actors and the driver
    """
    REQUEST_NEW_JOB = 0
    REQUEST_EVENT_RANGES = 1
    UPDATE_JOB = 2
    UPDATE_EVENT_RANGES = 3
    REQUEST_STATUS = 4
    PROCESS_DONE = 5
    IDLE = 6

    REPLY_OK = 200
    REPLY_NO_MORE_EVENT_RANGES = 201
    REPLY_NO_MORE_JOBS = 202


class ESEncoder(json.JSONEncoder):
    """
    JSON Encoder supporting serialization of event service data structures.
    """
    def default(self, o: object) -> dict:
        """
        Serialize event service data structure to json, use default encoder if the type of the object is unknown
        Args:
            o: object to serialize to json

        Returns:
            o encoded to json
        """
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


class PandaJobQueue(object):
    """
    Build from the reply to a job request. Harvester will provide the following JSON as a reply:

    {
        "pandaID": <jobspec>,
        ...
    }

    See PandaJob doc for the <jobspec> format
    """

    def __init__(self, jobs: dict = None) -> None:
        self.jobs = dict()
        self.distributed_jobs_ids = list()

        if jobs:
            self.add_jobs(jobs)

    def __getitem__(self, k: str) -> 'PandaJob':
        return self.jobs[k]

    def __setitem__(self, k: str, v: 'PandaJob') -> None:
        if isinstance(v, PandaJob):
            self.jobs[k] = v
        else:
            raise Exception(f"{v} is not of type {PandaJob}")

    def __iter__(self) -> Iterator[str]:
        return iter(self.jobs)

    def __len__(self) -> int:
        return len(self.jobs)

    def __contains__(self, k: str) -> bool:
        return self.has_job(k)

    def next_job_to_process(self) -> Union['PandaJob', None]:
        """
        Retrieve the next available job in the jobqueue. If the job is an eventservice job, it needs
        to have event ranges available otherwise it will not be considered as available

        Returns:
            PandaJob to process, None if no jobs are available
        """
        job_id, ranges_avail = self.next_job_id_to_process()

        if job_id is None:
            return None
        return self.jobs[job_id]

    def next_job_id_to_process(self) -> Tuple[Union[str, None], int]:
        """
        Retrieve the job worker_id and number of events available for the next job to process.
        Event service jobs with the most events available are chosen first, followed by non event-service jobs.

        Returns:
            Tuple of (job worker_id, nb event ranges) or (None, 0)
        """
        max_job_id = None
        max_avail = 0

        if len(self.jobs) == 0:
            return max_job_id, max_avail

        for jobID, job in self.jobs.items():
            if (('eventService' not in job or
                 job['eventService'].lower() == "false") and
                    jobID not in self.distributed_jobs_ids):
                self.distributed_jobs_ids.append(jobID)
                return jobID, 0
            if job.nranges_available() > max_avail:
                max_avail = job.nranges_available()
                max_job_id = jobID
        return max_job_id, max_avail

    def has_job(self, panda_id: str) -> bool:
        """
        Checks if the job worker_id is present in the queue

        Args:
            panda_id: job worker_id to check

        Returns:
            True if the job with specified worker_id is present in the job queue
        """
        return panda_id in self.jobs

    def add_jobs(self, jobs: Dict[str, dict]) -> None:
        """
        Adds specified jobs to the queue.

        Args:
            jobs: jobs dict as returned by harvester

        Returns:
            None
        """
        for jobID, jobDef in jobs.items():
            self.jobs[jobID] = PandaJob(jobDef)

    def get_event_ranges(self, panda_id: str) -> 'EventRangeQueue':
        """
        Retrieve the EventRangeQueue for the given panda job

        Args:
            panda_id: job worker_id

        Returns:
            EventRangeQueue holding ranges for the specified job
        """
        if panda_id in self.jobs:
            return self[panda_id].event_ranges_queue

    def process_event_ranges_update(self,
                                    ranges_update: 'EventRangeUpdate') -> None:
        """
        Update the range status
        Args:
            ranges_update: Range update provided by the payload

        Returns:
            None
        """
        for pandaID in ranges_update:
            self.get_event_ranges(pandaID).update_ranges(ranges_update[pandaID])

    def process_event_ranges_reply(self, reply: Dict[str, List[Dict]]) -> None:
        """
        Process an event ranges reply from harvester by adding ranges to each corresponding job already present in the
        queue. If an empty event list is received for a job, assume that no more events will be provided for this job

        Args:
            reply: new events received by harvester

        Returns:
            None
        """
        for pandaID, ranges in reply.items():
            if pandaID not in self.jobs:
                continue
            if not ranges:
                self[pandaID].no_more_ranges = True
            else:
                ranges_obj = list()
                for range_dict in ranges:
                    ranges_obj.append(EventRange.build_from_dict(range_dict))
                self.get_event_ranges(pandaID).concat(ranges_obj)

    @staticmethod
    def build_from_dict(jobs_dict: dict) -> 'PandaJobQueue':
        """
        Convert dict of jobs returned by harvester to a PandaJobQueue.
        Args:
            jobs_dict: dict loaded from json provided by harvester

        Returns:
            Job queue with jobs from jobs_dict already added in the queue
        """
        res = PandaJobQueue()
        res.add_jobs(jobs_dict)
        return res


class EventRangeQueue(object):
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
        """
        Init the queue
        """
        self.event_ranges_by_id: Dict[str, EventRange] = dict()
        self.rangesID_by_file: Dict[str, Dict[str, List[str]]] = dict()

    def __iter__(self) -> Iterator[str]:
        return iter(self.event_ranges_by_id)

    def __len__(self) -> int:
        return len(self.event_ranges_by_id)

    def __getitem__(self, k: str) -> 'EventRange':
        return self.event_ranges_by_id[k]

    def __setitem__(self, k: str, v: 'EventRange') -> None:
        if not isinstance(v, EventRange):
            raise Exception(f"{v} should be of type {EventRange}")
        if k != v.eventRangeID:
            raise Exception(f"Specified key '{k}' should be equals to the event range id '{v.eventRangeID}' ")
        if k in self.event_ranges_by_id:
            self.rangesID_by_file[self._get_file_from_id(k)][v.status].remove(k)
            self.event_ranges_by_id.pop(k)
        self.append(v)

    def __contains__(self, k: str) -> bool:
        return k in self.event_ranges_by_id

    @staticmethod
    def build_from_list(ranges_list: list) -> 'EventRangeQueue':
        """
        Build an EventRangeQueue from a list of event ranges sent by harvester

        Args:
            ranges_list: event ranges list for one job

        Returns:
            an EventRangesQueue holding event ranges present in 'ranges_list'
        """
        ranges_queue = EventRangeQueue()
        for r in ranges_list:
            ranges_queue.append(EventRange.build_from_dict(r))
        return ranges_queue

    def _get_file_from_id(self, range_id: str) -> str:
        return os.path.basename(self.event_ranges_by_id[range_id].PFN)

    def update_range_state(self, range_id: str, new_state: str) -> 'EventRange':
        """
        Update the status of an event range
        Args:
            range_id: range to update
            new_state: new event range state

        Returns:
            the updated event range
        """
        if range_id not in self.event_ranges_by_id:
            raise Exception(
                f"Trying to update non-existing eventrange {range_id}")

        event_range = self.event_ranges_by_id[range_id]
        file_name = self._get_file_from_id(range_id)

        self.rangesID_by_file[file_name][event_range.status].remove(range_id)
        event_range.status = new_state
        self.rangesID_by_file[file_name][event_range.status].append(range_id)
        return event_range

    def update_ranges(self, ranges_update: List[Dict]) -> None:
        """
        Process a range update sent by the payload by updating the range status to the new status. It is only
        possible to update event ranges which are in the assigned, or failed state, trying to update an unassigned or
         finished range will raise an exception

        Args:
            ranges_update: update sent by the payload

        Returns:
            None
        """
        for r in ranges_update:
            range_id = r['eventRangeID']
            range_status = r['eventStatus']
            if range_id not in self.event_ranges_by_id or \
                    range_id in self.rangesID_by_file[self._get_file_from_id(range_id)][EventRange.READY]:
                raise Exception(
                )
            self.update_range_state(range_id, range_status)

    def _get_ranges_count(self, state: str) -> int:
        count = 0
        for ranges in self.rangesID_by_file.values():
            count += len(ranges[state])
        return count

    def nranges_remaining(self) -> int:
        """
        Number of event ranges which are not finished or failed

        Returns:
            Number of event ranges which are not finished or failed
        """
        return len(self.event_ranges_by_id) - (self.nranges_done() +
                                               self.nranges_failed())

    def nranges_available(self) -> int:
        """
        Number of event ranges which can still be assigned to workers

        Returns:
            Number of event ranges that can still be assigned to workers
        """
        return self._get_ranges_count(EventRange.READY)

    def nranges_assigned(self) -> int:
        """
        Number of event ranges currently assigned to a worker

        Returns:
            Number of event ranges currently assigned to a worker
        """
        return self._get_ranges_count(EventRange.ASSIGNED)

    def nranges_failed(self) -> int:
        """
        Number of event ranges which failed

        Returns:
            Number of event ranges which failed
        """
        return self._get_ranges_count(EventRange.FAILED)

    def nranges_done(self) -> int:
        """
        Number of event ranges which finished successfully

        Returns:
            Number of event ranges which finished successfully
        """
        return self._get_ranges_count(EventRange.DONE)

    def append(self, event_range: Union[dict, 'EventRange']) -> None:
        """
        Append a single event range to the queue

        Args:
            event_range: event range to add to the queue

        Returns:
            None
        """
        if isinstance(event_range, dict):
            event_range = EventRange.build_from_dict(event_range)
        self.event_ranges_by_id[event_range.eventRangeID] = event_range
        file_name = self._get_file_from_id(event_range.eventRangeID)

        if file_name not in self.rangesID_by_file:
            files_ranges_states = dict()
            self.rangesID_by_file[file_name] = files_ranges_states
            for state in EventRange.STATES:
                files_ranges_states[state] = list()

        self.rangesID_by_file[file_name][event_range.status].append(
            event_range.eventRangeID)

    def concat(self, ranges: List[Union[dict, 'EventRange']]) -> None:
        """
        Concatenate a list of event ranges to the queue

        Args:
            ranges: list of event ranges to add to the queue

        Returns:
            None
        """
        for r in ranges:
            self.append(r)

    def _find_file_with_enough_ranges_ready(self, nranges: int) -> Union[None, str]:
        for file_name, ranges in self.rangesID_by_file.items():
            if len(ranges[EventRange.READY]) >= nranges:
                return file_name
        return None

    def get_next_ranges(self, nranges: int) -> List['EventRange']:
        """
        Dequeue event ranges. Event ranges which were dequeued are updated to the 'ASSIGNED' status
        and should be assigned to workers to be processed. In case more ranges are requested
        than there is available, assign all ranges.

        Args:
            nranges: number of ranges to get

        Returns:
            The list of event ranges assigned
        """
        res = list()
        nranges = min(nranges, self.nranges_available())

        file_name = self._find_file_with_enough_ranges_ready(nranges)
        if file_name:
            ids = self.rangesID_by_file[file_name][EventRange.READY][:nranges]
            for range_id in ids:
                res.append(self.update_range_state(range_id, EventRange.ASSIGNED))
            return res

        for ranges in self.rangesID_by_file.values():
            ids = ranges[EventRange.READY][:min(nranges, len(ranges[EventRange.READY]))]
            for range_id in ids:
                res.append(self.update_range_state(range_id, EventRange.ASSIGNED))
                if len(res) == nranges:
                    return res
        return res


class PandaJobUpdate(object):
    """
    Wrapper for jobUpdate

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


class EventRangeUpdate(object):
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
        """
        Wraps the range update dict in an object. The range update should be in the harvester-supported format.

        Args:
            range_update: range update
        """
        for v in range_update.values():
            if not isinstance(v, list):
                raise Exception(f"Expecting type list for element {v}")
        self.range_update = range_update

    def __len__(self) -> int:
        return len(self.range_update)

    def __iter__(self) -> Iterator[str]:
        return iter(self.range_update)

    def __str__(self) -> str:
        return json.dumps(self.range_update)

    def __getitem__(self, k: str) -> List[Dict]:
        return self.range_update[k]

    def __setitem__(self, k: str, v: list) -> None:
        if not isinstance(v, list):
            raise Exception(f"Expecting type list for element {v}")
        self.range_update[k] = v

    @staticmethod
    def build_from_dict(panda_id: str,
                        range_update: dict) -> 'EventRangeUpdate':
        """
        Parses a range_update dict to a format adapted to be sent to harvester.

        Args:
            panda_id: job worker_id associated to the range update
            range_update: the event ranges update sent by pilot 2

        Returns:
            EventRangeUpdate parsed to match harvester format
        """
        update_dict = dict()
        update_dict[panda_id] = list()

        if isinstance(
                range_update, dict
        ) and "zipFile" not in range_update and "eventRangeID" not in range_update:
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


class PandaJobRequest(object):
    """
    Wrapper for a job request.
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

    def __init__(self,
                 node: str = None,
                 disk_space: str = None,
                 working_group: str = None,
                 prod_source_label: str = None,
                 computing_element: str = None,
                 site_name: str = None,
                 resource_type: str = None,
                 mem: str = None,
                 cpu: str = None,
                 allow_other_country: str = None) -> None:
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


class EventRangeRequest(object):
    """
    Send event request to harvester. Event ranges for multiple jobs can be requested in a singled request.
    Harvester expects the following JSON schema:
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

    def __iter__(self) -> Iterator[str]:
        return iter(self.request)

    def __getitem__(self, k: str) -> dict:
        return self.request[k]

    def __str__(self) -> dict:
        return json.dumps(self.request)

    def add_event_request(self, panda_id, n_ranges, task_id, jobset_id) -> None:
        """
        Adds a job for which event ranges should be requested to the request object

        Args:
            panda_id: job worker_id for which event ranges should be requested
            n_ranges: number of ranges to request
            task_id: task worker_id provided in the job specification
            jobset_id: jobset worker_id provided in the job specification

        Returns:

        """
        self.request[panda_id] = {
            'pandaID': panda_id,
            'nRanges': n_ranges,
            'taskID': task_id,
            'jobsetID': jobset_id
        }

    @staticmethod
    def build_from_dict(request_dict: dict) -> 'EventRangeRequest':
        """
        Build a request object from a dict parsed from its json representation

        Args:
            request_dict: dict representation of the request

        Returns:
            EventRangeRequest wrapping the request dict
        """
        request = EventRangeRequest()
        request.request.update(request_dict)
        return request


class PandaJob(object):
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
        self._no_more_ranges = False

    @property
    def no_more_ranges(self) -> bool:
        """
        Indicates whether harvester can potentially sends more event ranges to this job

        Returns:
            True if harvester can still have more ranges to provide for this job
        """
        return self._no_more_ranges

    @no_more_ranges.setter
    def no_more_ranges(self, v: bool) -> None:
        self._no_more_ranges = v

    def nranges_available(self) -> int:
        """
        See Also:
            EventRangeQueue.nranges_available()
        """
        return self.event_ranges_queue.nranges_available()

    def get_next_ranges(self, nranges: int) -> List['EventRange']:
        """
        See Also:
            EventRangeQueue.get_next_ranges()
        """
        return self.event_ranges_queue.get_next_ranges(nranges)

    def get_pandaQueue(self) -> str:
        """
        Name of the panda queue from which harvester is retrieving jobs

        Returns:
            Name of the panda queue from which harvester is retrieving jobs
        """
        return self['destinationSE']

    def get_id(self) -> str:
        """
        Returns the job worker_id

        Returns:
            the job worker_id
        """
        return self['PandaID']

    def __str__(self) -> str:
        return json.dumps(self.job)

    def __getitem__(self, k: str) -> str:
        return self.job[k]

    def __setitem__(self, k: str, v: str) -> None:
        self.job[k] = v

    def __len__(self) -> int:
        return len(self.job)

    def __iter__(self) -> Iterator[str]:
        return iter(self.job)

    def __contains__(self, k: str) -> bool:
        return k in self.job


class EventRange(object):
    """
    Hold an event range:
    {
        "eventRangeID": _,
        "PFN": _,
        "lastEvent": _,
        "startEvent": _,
        "GUID": _
    }

    Note that while harvester returns a LFN field, AthenaMP needs PFN so the value fetched from LFN is assigned to PFN.
    Event ranges can be in one of the four states:

    READY: ready to be assigned to a worker
    ASSIGNED: currently assigned to a worker, waiting on an update
    DONE: the event range was processed successfully
    FAILED: the event range failed during processing
    """

    READY = "available"
    ASSIGNED = "running"
    DONE = "finished"
    FAILED = "failed"
    FATAL = "fatal"
    STATES = [READY, ASSIGNED, DONE, FAILED, FATAL]

    def __init__(self, event_range_id: str, start_event: int, last_event: int,
                 pfn: str, guid: str, scope: str) -> None:
        """
        Initialize the range

        Args:
            event_range_id: the range worker_id
            start_event: first event index in PFN
            last_event: last event index in PFN
            pfn: physical path to the event file
            guid: file GUID
            scope: event scope
        """
        self.lastEvent = last_event
        self.eventRangeID = event_range_id
        self.startEvent = start_event
        self.PFN = pfn
        self.GUID = guid
        self.scope = scope
        self.status = EventRange.READY
        self.retry = 0

    def set_assigned(self) -> None:
        """
        Set current state to ASSIGNED

        Returns:
            None
        """
        self.status = EventRange.ASSIGNED

    def set_done(self) -> None:
        """
        Set current state to DONE

        Returns:
            None
        """
        self.status = EventRange.DONE

    def set_failed(self) -> None:
        """
        Set current state to FAILED

        Returns:
            None
        """
        self.status = EventRange.FAILED

    def nevents(self) -> int:
        """
        Returns the number of events in the range. It should always be one

        Returns:
            number of events in the range
        """
        return self.lastEvent - self.startEvent + 1

    def __str__(self) -> str:
        """
        Dump the dict serialization to a json string

        Returns:
            json dump of self.to_dict()
        """
        return json.dumps(self.to_dict())

    def to_dict(self) -> dict:
        """
        Serialize the range to a dict, omitting the state as it is only used internally and not required by AthenaMP

        Returns:
            dict serialization of the range
        """
        return {
            'PFN': self.PFN,
            'lastEvent': self.lastEvent,
            'eventRangeID': self.eventRangeID,
            'startEvent': self.startEvent,
            'GUID': self.GUID
        }

    @staticmethod
    def build_from_dict(event_ranges_dict: dict) -> 'EventRange':
        """
        Construct an event range from a dict returned by harvester

        Args:
            event_ranges_dict: dict representing the event range

        Returns:
            EventRange object
        """
        return EventRange(
            event_ranges_dict['eventRangeID'], event_ranges_dict['startEvent'],
            event_ranges_dict['lastEvent'],
            event_ranges_dict.get('PFN', event_ranges_dict.get('LFN', None)),
            event_ranges_dict['GUID'], event_ranges_dict['scope'])
