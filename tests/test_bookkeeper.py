import pytest
from Raythena.drivers.esdriver import BookKeeper
from Raythena.actors.loggingActor import LoggingActor


@pytest.mark.usefixtures("requires_ray")
class TestBookKeeper:

    def test_fetch_event_ranges(self, is_eventservice, config, sample_multijobs, njobs, nevents, sample_ranges):
        if not is_eventservice:
            pytest.skip()
        worker_ids = [f"w_{i}" for i in range(10)]

        logging_actor = LoggingActor.remote(config)
        bookKeeper = BookKeeper(logging_actor, config)
        assert not bookKeeper.has_jobs_ready()

        bookKeeper.add_jobs(sample_multijobs)
        assert bookKeeper.has_jobs_ready() != is_eventservice
        for pandaID in sample_multijobs:
            assert not bookKeeper.is_flagged_no_more_events(pandaID)
            assert bookKeeper.n_ready(pandaID) == 0

        bookKeeper.add_event_ranges(sample_ranges)
        assert bookKeeper.has_jobs_ready()
        for pandaID in sample_multijobs:
            assert bookKeeper.n_ready(pandaID) == nevents

        for wid in worker_ids:
            assert not bookKeeper.fetch_event_ranges(wid, 100)

        assigned_workers = worker_ids[:int(len(worker_ids) / 2)]
        for wid in assigned_workers:
            job = bookKeeper.assign_job_to_actor(wid)
            assert job['PandaID'] in sample_multijobs
            ranges = bookKeeper.fetch_event_ranges(wid, int(nevents / len(assigned_workers)))
            assert ranges

    def test_process_event_ranges_update(self):
        pass

    def process_actor_end(self):
        pass
