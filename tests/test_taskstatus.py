import os

from raythena.utils.bookkeeper import TaskStatus
from raythena.utils.eventservice import EventRange, PandaJob


class TestTaskStatus:

    def test_save_restore_status(self, tmp_path, config, sample_job, sample_ranges):
        config.ray["outputdir"] = tmp_path
        job = PandaJob(list(sample_job.values())[0])
        ts = TaskStatus(job, config)

        ranges_list = list(sample_ranges.values())[0]
        for r in ranges_list:
            ts.set_eventrange_simulated(EventRange.build_from_dict(r))

        fname = EventRange.build_from_dict(ranges_list[0]).PFN
        ts.set_file_merged(fname, f"{fname}-MERGED")
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
            ts.set_eventrange_simulated(EventRange.build_from_dict(r))

        # need to save as set_event_range_simulated is lazy
        ts.save_status()
        assert len(ts._status) == nfiles
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
        assert len(ts._status) == nfiles
        assert ts.get_nfailed() == nevents
        assert ts.get_nmerged() == 0

    def test_set_merged(self, nfiles, nevents, tmp_path, config, sample_job, sample_ranges):
        config.ray["outputdir"] = tmp_path
        job = PandaJob(list(sample_job.values())[0])
        ts = TaskStatus(job, config)
        ts._events_per_file = nevents / nfiles
        ranges_list = list(sample_ranges.values())[0]
        for r in ranges_list:
            ts.set_eventrange_simulated(EventRange.build_from_dict(r))

        fname = EventRange.build_from_dict(ranges_list[0]).PFN
        ts.set_file_merged(fname, f"{fname}-MERGED")
        ts.save_status()
        assert ts.get_nsimulated() == (nfiles - 1) * nevents / nfiles
        assert len(ts._status[TaskStatus.MERGED]) == 1
        assert ts.get_nmerged() == nevents / nfiles

        fname = EventRange.build_from_dict(ranges_list[1]).PFN
        ts.set_file_merged(fname, f"{fname}-MERGED")
        ts.save_status()
        assert ts.get_nsimulated() == (nfiles - 2) * nevents / nfiles
        assert len(ts._status[TaskStatus.MERGED]) == 2
        assert ts.get_nmerged() == 2 * nevents / nfiles
        print(ts._status)
