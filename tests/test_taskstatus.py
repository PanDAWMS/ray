from raythena.utils.bookkeeper import TaskStatus
from raythena.utils.eventservice import EventRange, PandaJob


class TestTaskStatus:

    def test_save_restore_status(self, nfiles, tmp_path, config, sample_job, sample_ranges):
        config.ray["outputdir"] = tmp_path
        job = PandaJob(list(sample_job.values())[0])
        ts = TaskStatus(job, config)
        ranges = list(sample_ranges.values())[0]
        events_per_file = int(job['emergeSpec']['nEventsPerOutputFile'])
        hits_per_file = int(job['nEventsPerInputFile'])
        assert events_per_file % hits_per_file == 0
        n_output_per_input_file = events_per_file // hits_per_file
        offset = nfiles
        file_no = 0
        for i in range(0, n_output_per_input_file):
            ranges_list = []
            for j in range(hits_per_file):
                ranges_list.append(ranges[file_no + (i * offset) + (i + j) * offset])
            ranges_map = {}
            arbitrary_range = EventRange.build_from_dict(ranges_list[0])
            fname = arbitrary_range.PFN
            outputfile = f"{fname}-MERGED-{arbitrary_range.eventRangeID}"
            for r in ranges_list:
                event_range = EventRange.build_from_dict(r)
                ranges_map[event_range.eventRangeID] = TaskStatus.build_eventrange_dict(event_range, fname)
                ts.set_eventrange_simulated(event_range, "outputfile")

            ts.set_file_merged(fname, outputfile, ranges_map)
        ts.save_status()
        ts2 = TaskStatus(job, config)
        print(ts._status)
        assert ts._status == ts2._status

    def test_set_processed(self, nfiles, nevents, tmp_path, config, sample_job, sample_ranges):
        config.ray["outputdir"] = tmp_path
        job = PandaJob(list(sample_job.values())[0])
        ts = TaskStatus(job, config)

        ranges_list = list(sample_ranges.values())[0]
        for r in ranges_list:
            ts.set_eventrange_simulated(EventRange.build_from_dict(r), "outputfile")

        # need to save as set_event_range_simulated is lazy
        ts.save_status()
        assert len(ts._status[TaskStatus.SIMULATED]) == nfiles
        assert ts.get_nsimulated() == nevents
        assert ts.get_nmerged() == 0

    def test_set_failed(self, nfiles, nevents, tmp_path, config, sample_job, sample_ranges):
        config.ray["outputdir"] = tmp_path
        job = PandaJob(list(sample_job.values())[0])
        ts = TaskStatus(job, config)

        ranges_list = list(sample_ranges.values())[0]
        for r in ranges_list:
            ts.set_eventrange_failed(EventRange.build_from_dict(r))

        # need to save as set_event_range_simulated is lazy
        ts.save_status()
        assert len(ts._status[TaskStatus.FAILED]) == nfiles
        assert ts.get_nfailed() == nevents
        assert ts.get_nmerged() == 0

    def test_set_merged(self, nfiles, nevents, tmp_path, config, sample_job, sample_ranges):
        config.ray["outputdir"] = tmp_path
        job = PandaJob(list(sample_job.values())[0])
        ts = TaskStatus(job, config)

        ranges = list(sample_ranges.values())[0]
        for e in ranges:
            er = EventRange.build_from_dict(e)
            ts.set_eventrange_simulated(er, f"outputfile-{er.eventRangeID}")

        events_per_file = int(job['emergeSpec']['nEventsPerOutputFile'])
        hits_per_file = int(job['nEventsPerInputFile'])
        assert events_per_file % hits_per_file == 0
        n_output_per_input_file = events_per_file // hits_per_file
        offset = nfiles
        file_no = 0
        for i in range(0, n_output_per_input_file):
            ranges_list = []
            for j in range(hits_per_file):
                ranges_list.append(ranges[file_no + (i * offset) + (i + j) * offset])
            arbitrary_range = EventRange.build_from_dict(ranges_list[0])
            fname = arbitrary_range.PFN
            outputfile = f"{fname}-MERGED-{arbitrary_range.eventRangeID}"
            ranges_map = {}
            for r in ranges_list:
                event_range = EventRange.build_from_dict(r)
                ranges_map[event_range.eventRangeID] = TaskStatus.build_eventrange_dict(event_range, f"outputfile-{event_range.eventRangeID}")

            ts.set_file_merged(fname, outputfile, ranges_map)
        ts.save_status()
        print(ts._status)
        assert ts.get_nsimulated() == (nfiles - 1) * nevents / nfiles
        assert len(ts._status[TaskStatus.MERGED]) == 1
        assert ts.get_nmerged() == nevents / nfiles

        file_no = 1
        for i in range(0, n_output_per_input_file):
            ranges_list = []
            for j in range(hits_per_file):
                ranges_list.append(ranges[file_no + (i * offset) + (i + j) * offset])
            arbitrary_range = EventRange.build_from_dict(ranges_list[0])
            fname = arbitrary_range.PFN
            outputfile = f"{fname}-MERGED-{arbitrary_range.eventRangeID}"
            ranges_map = {}
            for r in ranges_list:
                event_range = EventRange.build_from_dict(r)
                ranges_map[event_range.eventRangeID] = TaskStatus.build_eventrange_dict(event_range, f"outputfile-{event_range.eventRangeID}")
            ts.set_file_merged(fname, outputfile, ranges_map)
        ts.save_status()
        print(ts._status)
        assert ts.get_nsimulated() == (nfiles - 2) * nevents / nfiles
        assert len(ts._status[TaskStatus.MERGED]) == 2
        assert ts.get_nmerged() == 2 * nevents / nfiles
