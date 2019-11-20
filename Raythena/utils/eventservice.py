import json


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

    def default(self, o):

        if isinstance(o, PandaJobQueue):
            return o.jobs
        if isinstance(o, EventRangeQueue):
            return o.eventranges_by_id

        if isinstance(o, PandaJobUpdate):
            raise NotImplementedError()
        if isinstance(o, EventRangeUpdate):
            return o.range_update

        if isinstance(o, PandaJobRequest):
            raise o.to_dict()
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

    def __init__(self, jobs=None):
        self.jobs = dict()

        if jobs:
            self.add_jobs(jobs)

    def __getitem__(self, k):
        return self.jobs[k]

    def __setitem__(self, k, v):
        if isinstance(v, PandaJob):
            self.jobs[k] = v
        else:
            raise Exception(f"{v} is not of type {PandaJob}")

    def __iter__(self):
        return iter(self.jobs)

    def __len__(self):
        return len(self.jobs)

    def __contains__(self, k):
        return self.has_job(k)

    def next_job_to_process(self):
        jobID, ranges_avail = self.jobid_next_job_to_process()

        if jobID is None or ranges_avail == 0:
            return None
        return self.jobs[jobID]

    def jobid_next_job_to_process(self):
        """
        Return the jobid that workers requesting new jobs should work on which is defined by the job javing the most events available.
        """
        max_jobID = None
        max_avail = 0

        if len(self.jobs) == 0:
            return max_jobID, max_avail

        for jobID, job in self.jobs.items():
            if job.nranges_available() > max_avail:
                max_avail = job.nranges_available()
                max_jobID = jobID
        return max_jobID, max_avail

    def has_job(self, pandaID):
        return pandaID in self.jobs

    def add_jobs(self, jobs):
        for jobID, jobDef in jobs.items():
            self.jobs[jobID] = PandaJob(jobDef)

    def get_eventranges(self, pandaID):
        if pandaID in self.jobs:
            return self[pandaID].event_ranges_queue

    def process_event_ranges_update(self, rangesUpdate):
        for pandaID in rangesUpdate:
            self.get_eventranges(pandaID).update_ranges(rangesUpdate[pandaID])

    def process_event_ranges_reply(self, reply):
        """
        Process an event ranges reply from harvester by adding ranges to each corresponding job already present in the queue
        """
        for pandaID, ranges in reply.items():
            if pandaID not in self.jobs:
                continue
            if not ranges:
                self.get_eventranges(pandaID).no_more_ranges = True
            else:
                ranges_obj = list()
                for range_dict in ranges:
                    ranges_obj.append(EventRange.build_from_dict(range_dict))
                self.get_eventranges(pandaID).concat(ranges_obj)

    @staticmethod
    def build_from_dict(jobsdict):
        res = PandaJobQueue()
        res.add_jobs(jobsdict)
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

    def __init__(self):
        self.eventranges_by_id = dict()
        self.rangesID_by_state = dict()
        self._no_more_ranges = False
        for state in EventRange.STATES:
            self.rangesID_by_state[state] = list()

    @property
    def no_more_ranges(self):
        return self._no_more_ranges

    @no_more_ranges.setter
    def no_more_ranges(self, v):
        self._no_more_ranges = v

    def __iter__(self):
        return iter(self.eventranges_by_id)

    def __len__(self):
        return len(self.eventranges_by_id)

    def __getitem__(self, k):
        return self.eventranges_by_id[k]

    def __setitem__(self, k, v):
        if not isinstance(v, EventRange):
            raise Exception(f"{v} should be of type {EventRange}")
        self.eventranges_by_id[k] = v

    def __contains__(self, k):
        return k in self.eventranges_by_id

    @staticmethod
    def build_from_list(ranges_list):
        ranges_queue = EventRangeQueue()
        for r in ranges_list:
            ranges_queue.add(EventRange.build_from_dict(r))

    def update_range_state(self, rangeID, new_state):
        if rangeID not in self.eventranges_by_id:
            raise Exception(f"Trying to update non-existing eventrange {rangeID}")

        r = self.eventranges_by_id[rangeID]
        self.rangesID_by_state[r.status].remove(rangeID)
        r.status = new_state
        self.rangesID_by_state[r.status].append(rangeID)

    def update_ranges(self, rangesUpdate):
        for r in rangesUpdate:
            rangeID = r['eventRangeID']
            rangeStatus = r['eventStatus']
            if rangeStatus == "running" and rangeID not in self.rangesID_by_state[EventRange.ASSIGNED]:
                raise Exception(f"Unexpected state: {rangeID} updated as running is not assigned")
            if rangeStatus == "finished":
                self.update_range_state(rangeID, EventRange.DONE)
            else:
                self.update_range_state(rangeID, EventRange.FAILED)

    def nranges_remaining(self):
        return len(self.eventranges_by_id) - (self.nranges_done() + self.nranges_failed())

    def nranges_available(self):
        return len(self.rangesID_by_state[EventRange.READY])

    def nranges_assigned(self):
        return len(self.rangesID_by_state[EventRange.ASSIGNED])

    def nranges_failed(self):
        return len(self.rangesID_by_state[EventRange.FAILED])

    def nranges_done(self):
        return len(self.rangesID_by_state[EventRange.DONE])

    def append(self, eventrange):
        self.eventranges_by_id[eventrange.eventRangeID] = eventrange
        self.rangesID_by_state[eventrange.status].append(eventrange.eventRangeID)

    def concat(self, ranges):
        for r in ranges:
            self.append(r)

    def process_ranges_update(self, ranges_update):
        for r in ranges_update:
            self.update_range_state(r['eventRangeID'],
                                    EventRange.DONE if r['eventStatus'] == 'finished' else EventRange.FAILED)

    def get_next_ranges(self, nranges):
        res = list()
        nranges = min(nranges, len(self.rangesID_by_state[EventRange.READY]))
        for i in range(nranges):
            rangeID = self.rangesID_by_state[EventRange.READY].pop()
            r = self.eventranges_by_id[rangeID]
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

    def __init__(self):
        super().__init__()


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

    The JSON schema that should be send is as shown below.

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

    def __init__(self, range_update):
        for v in range_update.values():
            if not isinstance(v, list):
                raise Exception(f"Expecting type list for element {v}")
        self.range_update = range_update

    def __len__(self):
        return len(self.range_update)

    def __iter__(self):
        return iter(self.range_update)

    def __getitem__(self, k):
        return self.range_update[k]

    def __setitem__(self, k, v):
        if not isinstance(v, list):
            raise Exception(f"Expecting type list for element {v}")
        self.range_update[k] = v

    @staticmethod
    def build_from_dict(pandaID, range_update):

        update_dict = dict()
        update_dict[pandaID] = list()

        for f in range_update:
            fileInfo = f.get('zipFile', None)
            rangesInfo = f.get('eventRanges', None)
            fileData = dict()
            if fileInfo['lfn'].find('.root') > -1:
                ftype = "es_output"
            else:
                ftype = "zip_output"

            if fileInfo:
                fileData['path'] = fileInfo['lfn']
                fileData['type'] = ftype
                fileData['chksum'] = fileInfo['adler32']
                fileData['fsize'] = fileInfo['fsize']
                fileData['guid'] = None

            if rangesInfo:
                for rangeInfo in rangesInfo:
                    elt = dict()
                    elt['eventRangeID'] = rangeInfo['eventRangeID']
                    elt['eventStatus'] = rangeInfo['eventStatus']
                    elt.update(fileData)
                    update_dict[pandaID].append(elt)
            else:
                update_dict[pandaID].append(fileData)

        return EventRangeUpdate(update_dict)


class PandaJobRequest:
    """
    Pilot2 requests job using the following JSON schema:
    {
        "node": _,
        "diskSpace": _,
        "workingGroup": _,
        "prodSourceLabel: _,
        "computingElement: _,
        "siteName": _,
        "resourceType: _,
        "mem": _,
        "cpu": _,
        "allowOtherCountry": _
    }

    Note that harvester will ignore the content of the job request file and simply check if it exists
    """
    def __init__(self, node=None, diskSpace=None, workingGroup=None, prodSourceLabel=None,
                 computingElement=None, siteName=None, resourceType=None, mem=None,
                 cpu=None, allowOtherCountry=None):
        self.node = node
        self.diskSpace = diskSpace
        self.workingGroup = workingGroup
        self.prodSourceLabel = prodSourceLabel
        self.computingElement = computingElement
        self.siteName = siteName
        self.resourceType = resourceType
        self.mem = mem
        self.cpu = cpu
        self.allowOtherCountry = allowOtherCountry

    def to_dict(self):
        return {}


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
    def __init__(self):
        self.request = dict()

    def __len__(self):
        return len(self.request)

    def __iter__(self):
        return iter(self.request)
    
    def __getitem__(self, k):
        return self.request[k]

    def __str__(self):
        return json.dumps(self.request)

    def add_event_request(self, pandaID, nRanges, taskId, jobsetID):
        self.request[pandaID] = {'pandaID': pandaID, 'nRanges': nRanges, 'taskId': taskId, 'jobsetID': jobsetID}

    @staticmethod
    def build_from_dict(request_dict):
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

    def __init__(self, jobDef):
        self.job = jobDef
        self.event_ranges_queue = EventRangeQueue()

    def nranges_available(self):
        return self.event_ranges_queue.nranges_available()

    def get_next_ranges(self, nranges):
        return self.event_ranges_queue.get_next_ranges(nranges)

    def get_pandaQueue(self):
        return self['destinationSE']

    def get_id(self):
        return self['PandaID']

    def __str__(self):
        return json.dumps(self.job)

    def __getitem__(self, k):
        return self.job[k]

    def __setitem__(self, k, v):
        self.job[k] = v

    def __len__(self):
        return len(self.job)

    def __iter__(self):
        return iter(self.job)

    def __contains__(self, k):
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

    def __init__(self, eventRangeID, startEvent, lastEvent, PFN, GUID, scope):
        self.lastEvent = lastEvent
        self.eventRangeID = eventRangeID
        self.startEvent = startEvent
        self.PFN = PFN
        self.GUID = GUID
        self.scope = scope
        self.status = EventRange.READY
        self.retry = 0

    def set_assigned(self):
        self.status = EventRange.ASSIGNED

    def set_done(self):
        self.status = EventRange.DONE

    def set_failed(self):
        self.status = EventRange.FAILED

    def nevents(self):
        return self.lastEvent - self.startEvent + 1

    def __str__(self):
        return json.dumps(self.to_dict())

    def to_dict(self):
        return {
            'PFN': self.PFN,
            'lastEvent': self.lastEvent,
            'eventRangeID': self.eventRangeID,
            'startEvent': self.startEvent,
            'GUID': self.GUID
        }

    @staticmethod
    def build_from_dict(eventRangeDict):
        return EventRange(eventRangeDict['eventRangeID'],
                          eventRangeDict['startEvent'],
                          eventRangeDict['lastEvent'],
                          eventRangeDict.get('PFN', eventRangeDict.get('LFN', None)),
                          eventRangeDict['GUID'],
                          eventRangeDict['scope'])
