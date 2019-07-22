import os
import re

from subprocess import *
from time import sleep, strftime
import collections

import ray
import yampl

from Raythena.HelperFunctions import getSetupCommand


class Driver(object):

    def __init__(self, asetup,
                 event_ranges,
                 max_ranges,
                 number_of_actors,
                 cores_per_actor,
                 merge_event_size,
                 max_retries,
                 run_dir,
                 merge_dir,
                 geometry_version,
                 physics_list,
                 conditions_tag,
                 proxy_evnt_file,
                 yampl_communication_channel,
                 extra_pre_exec=[],
                 extra_setup_commands=[]):
        # Athena release
        self.asetup = asetup
        # Maximum number of ranges to process
        self.max_ranges = max_ranges
        # number of actors
        self.number_of_actors = number_of_actors
        # number of cores per node
        self.cores_per_actor = cores_per_actor
        # Number of events to merge into a merged dataset
        self.merge_event_size = merge_event_size
        # Maximum number of retries
        self.max_retries = max_retries
        # Directory where AthenaMP and merge jobs run
        self.run_dir = run_dir
        # Directory where merged datasets are saved
        self.merge_dir = merge_dir
        # geometry version
        self.geometry_version = geometry_version
        # physics list
        self.physics_list = physics_list
        # conditions tag
        self.conditions_tag = conditions_tag
        # proxy EVNT file
        self.proxy_evnt_file = proxy_evnt_file
        # extra preIncludes for the job
        self.extra_pre_exec = extra_pre_exec
        # extra setup commands
        self.extra_setup_commands = extra_setup_commands

        # Event ranges to process
        self.eventRanges = {}
        # Event ranges currently being processed
        self.submittedEventRanges = {}
        # Failed event ranges
        self.failedEventRanges = {}
        # Number of times a range was retreid
        self.numberOfRetries = collections.Counter()
        # Number of processed ranges (including fails)
        self.processedRanges = 0
        # Successfully finished jobs
        self.results = []
        # Merged datasets
        self.mergedDatasets = []
        # all AthenaMP actors
        self.actors = []
        # ready AthenaMP actors
        self.readyActors = []

        # loop through the input event ranges to verify some
        # things and put them in various dictionaries
        self.eventsPerRange = []
        import ast
        for r in open(event_ranges).readlines():
            dictionary = ast.literal_eval(r)[0]
            assert type(dictionary) == dict and 'lastEvent' in dictionary and 'startEvent' in dictionary, \
                "unexpected eventRange structure"
            assert dictionary["eventRangeID"] not in self.eventRanges.keys(), \
                "two event ranges have the same name: %s" % dictionary["eventRangeID"]
            self.eventRanges[dictionary["eventRangeID"]] = r
            self.eventsPerRange += [1 + dictionary['lastEvent'] - dictionary['startEvent']]
            if self.max_ranges > 0 and len(self.eventRanges) >= self.max_ranges:
                break

        # Number of all input event ranges
        self.numRanges = len(self.eventRanges)

        # are all ranges equally long?
        assert self.eventsPerRange.count(self.eventsPerRange[0]) == len(self.eventsPerRange), \
            "ranges with different number of events not supported: " + \
            " ".join([str(x) for x in self.eventsPerRange])
        self.eventsPerRange = self.eventsPerRange[0]

        assert self.merge_event_size % self.eventsPerRange == 0, \
            "cannot create merged datasets with %s events with event range length of %s" % (
                self.merge_event_size, self.eventsPerRange)

        # create AthenaMP actors
        for i in range(self.number_of_actors):
            self.createActor('Athena%s' % i, '%s_%s' %
                             (yampl_communication_channel, i))

    def execute(self):
        # send a message to all AthenaMP instances at once
        remaining_ids = [a.getMessage.remote() for a in self.popReadyActors()]

        # start processing the ones that respond first until all are processed
        ready_ids, remaining_ids = ray.wait(remaining_ids)

        while ready_ids:
            # process actors that already returned a message
            self.process(ready_ids)

            # progress tracking
            print ("%s / %s" % (self.processedRanges, self.numRanges))

            # exit contidion
            if self.processedRanges == self.numRanges:
                break

            # send a message to all AthenaMPs that just got processed
            remaining_ids += [a.getMessage.remote()
                              for a in self.popReadyActors()]

            # start processing the ones that respond first until all are processed
            ready_ids, remaining_ids = ray.wait(remaining_ids)

        # We finished processing all ranges. Wait for merge
        # jobs to finish and print out a summary.
        self.finalize()

    def process(self, ready_ids):
        for athena, result in ray.get(ready_ids):

            # make this athena available for messaging again
            self.readyActors += [self[athena]]

            # process the result
            if result == None:
                self.submitNextRange(athena)
            else:
                self.appendResult(result)

            # remote merge tasks
            if len(self.results)*self.eventsPerRange >= self.merge_event_size:
                self.mergeEvents()

    def submitNextRange(self, athena):
        if self.eventRanges:
            rangeID = next(iter(self.eventRanges))
            eventRange = self.eventRanges.pop(rangeID)
            self.submittedEventRanges[rangeID] = eventRange
            self[athena].submitRange.remote(eventRange)
        else:
            self[athena].terminateAthenaMP.remote()

    def appendResult(self, result):
        if "ERR_ATHENAMP_PROCESS" in result:
            self.handleError(result)
        else:
            self.results += [(result)]
            self.processedRanges += 1

    def mergeEvents(self):
        events = [self.results.pop().split(",")[0]
                  for _ in range(self.merge_event_size/self.eventsPerRange)]
        outName = "myHITS.pool.root.Merged-%s" % (
            (self.eventsPerRange*self.processedRanges)/self.merge_event_size)
        self.mergedDatasets += [mergeJob.remote(
            self.asetup, self.extra_setup_commands, self.run_dir, events, outName, self.merge_dir)]

    def handleError(self, result):
        # only works with a certain structure of errors, e.g.:
        # ERR_ATHENAMP_PROCESS Range-1000: Not found in the input file
        # ERR_ATHENAMP_PROCESS Range-49: Seek failed
        expression = re.findall('ERR_ATHENAMP_PROCESS (.*):', result)
        assert len(expression) == 1, "Error not recognised..."
        rangeID = expression[0]
        # save the ERROR message
        if not rangeID in self.failedEventRanges.keys():
            self.failedEventRanges[rangeID] = [(result)]
        else:
            self.failedEventRanges[rangeID] += [(result)]
        # do not process it again
        # if already retried a max a mount of times
        if self.numberOfRetries[rangeID] >= self.max_retries:
            print ("Range: %s failed %s times. Will not process it." % (
                rangeID, self.max_retries))
            # increase the processed ranges counter
            self.processedRanges += 1
        # put the range back in the 'to-be-processed' pool
        else:
            self.numberOfRetries[rangeID] += 1
            self.eventRanges[rangeID] = self.submittedEventRanges.pop(rangeID)

    def popReadyActors(self):
        # pop all ready actors...
        readyActors, self.readyActors = self.readyActors, []
        return readyActors

    def finalize(self):
        print ("--- merged output ---")
        for Output, Input in ray.get(self.mergedDatasets):
            print Output
            for inp in sorted(Input.split(",")):
                print ("    %s" % inp)

        print ("--- failed event ranges ---")
        for eventRange, msg in self.failedEventRanges.iteritems():
            print ("%s %s" % (eventRange, msg))

        print ("--- un-merged event ranges ---")
        for eventRange in self.results:
            print (eventRange)

    def createActor(self, name, yampl_communication_channel):
        actor = AthenaMP.remote(name,
                                self.asetup,
                                self.extra_setup_commands,
                                self.run_dir,
                                self.cores_per_actor,
                                self.geometry_version,
                                self.physics_list,
                                self.conditions_tag,
                                self.proxy_evnt_file,
                                self.extra_pre_exec,
                                yampl_communication_channel)
        self.actors += [actor]
        self.readyActors += [actor]
        setattr(self, name, actor)

    # for easier access to AthenaMP actors
    def __getitem__(self, key):
        return getattr(self, key)


@ray.remote
def mergeJob(asetup, extra_setup_commands, run_dir, datasets, outfile, outDir):
    folder_name = outfile

    out = os.path.join(outDir, outfile)
    inDS = ",".join(datasets)

    command = getSetupCommand(
        asetup, extra_setup_commands, os.path.join(run_dir, folder_name))
    command += "mkdir -p %s;" % outDir
    command += """HITSMerge_tf.py \
        '--inputHITSFile' '%s' \
        '--outputHITS_MRGFile' '%s' \
        2>&1 > log.mergeJob""" % (inDS, out)

    # launch merge job and wait
    print ("start merge job with output %s for datasets %s" % (
        out, inDS))

    p = Popen(command, shell=True, stdin=PIPE,
              stdout=PIPE, stderr=PIPE)

    p.communicate()

    # return the exit code and path to output file
    return (out, inDS)


@ray.remote
class AthenaMP(object):

    def __init__(self, name,
                 asetup,
                 extra_setup_commands,
                 run_dir,
                 num_cores,
                 geometry_version,
                 physics_list,
                 conditions_tag,
                 proxy_evnt_file,
                 extra_pre_exec,
                 yampl_communication_channel):
        self.name = name
        self.asetup = asetup
        self.extra_setup_commands = extra_setup_commands
        self.run_dir = run_dir
        self.num_cores = num_cores
        self.geometry_version = geometry_version
        self.physics_list = physics_list
        self.conditions_tag = conditions_tag
        self.proxy_evnt_file = proxy_evnt_file
        self.extra_pre_exec = extra_pre_exec
        self.yampl_communication_channel = yampl_communication_channel

        # Received results from AthenaMP
        self.results = []

        # Generate the Bash command for launching AthenaMP
        self.makeAthenaMPCommand()

        # Launch AthenaMP and continue
        p = Popen(self.command, shell=True, stdin=None,
                  stdout=None, stderr=None, close_fds=True)

        # Create a yampl server object
        self.createYamplServer()

    def getMessage(self):
        """
        Tries to retrive a message from AthenaMP
            returns <ready_state>, <received_message>
        """
        while True:
            try:
                size, buf = self.yampl_server.try_recv_raw()
                if size == -1:
                    # no new message received
                    sleep(0.1)
                elif buf == "AthenaMP_has_finished":
                    print ("[%s] This should not happen: %s" % (self.name, buf))
                    return None, None
                elif buf == "Ready for events":
                    print ("[%s] AthenaMP is ready for an event range" % self.name)
                    return self.name, None
                else:
                    print ("[%s] Received message: %s at %s" % (self.name, buf, strftime("%H:%M:%S")))
                    return self.name, buf

            except Exception, e:
                # this has never happened so far...
                print ("Caught exception: %s" % e)
                sleep(1)

    def submitRange(self, eventRange):
        self.sendYamplMessage(eventRange)

    def terminateAthenaMP(self):
        print ("Closing AthenaMP... %s" % self.name)
        self.sendYamplMessage("No more events")

    def sendYamplMessage(self, message):
        self.yampl_server.send_raw(message)
        print ("Sent %s" % message)

    def createYamplServer(self):
        # Create the server socket
        try:
            self.yampl_server = yampl.ServerSocket(
                self.yampl_communication_channel, "local")
        except Exception, e:
            print ("Could not create Yampl server socket: %s" % e)
        else:
            print ("Created a Yampl server socket")

    def makeAthenaMPCommand(self):
        run_dir = "%s/%s/" % (self.run_dir, self.name)
        command = getSetupCommand(
            self.asetup, self.extra_setup_commands, run_dir)

        preExec_YamplConfig = """'from AthenaMP.AthenaMPFlags import jobproperties as jps; \
                                  jps.AthenaMPFlags.EventRangeChannel.set_Value_and_Lock("%s")'""" % \
            self.yampl_communication_channel

        # copy a proxy file for initialization of IO locally
        command += """cp %s .;
        """ % self.proxy_evnt_file

        # add any extra preExecutes
        extra_pre_exec = """"""
        for x in self.extra_pre_exec:
            extra_pre_exec += """ '%s'""" % x

        command += """ATHENA_CORE_NUMBER=%s AtlasG4_tf.py \
            '--inputEvgenFile' '%s' \
            '--outputHitsFile' 'myHITS.pool.root' '--maxEvents=-1' '--randomSeed=1234' \
            '--geometryVersion' '%s' '--physicsList' '%s' '--conditionsTag' '%s' \
            '--preInclude' 'sim:SimulationJobOptions/preInclude.FrozenShowersFCalOnly.py,SimulationJobOptions/preInclude.BeamPipeKill.py' \
            '--athenaopts="--preloadlib=${ATLASMKLLIBDIR_PRELOAD}/libimf.so"' \
            '--eventService=True' \
            '--multiprocess' \
            '--preExec' %s %s \
            2>&1 > /dev/null """ % (self.num_cores,
                                    os.path.basename(self.proxy_evnt_file),
                                    self.geometry_version,
                                    self.physics_list,
                                    self.conditions_tag,
                                    extra_pre_exec, preExec_YamplConfig)

        self.command = command
