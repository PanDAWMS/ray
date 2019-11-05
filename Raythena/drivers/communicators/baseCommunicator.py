
class BaseCommunicator:

    def __init__(self, config):
        self.config = config

    def get_event_ranges(self, evnt_request):
        raise NotImplementedError("Base method not implemented")

    def get_job(self, job_request):
        raise NotImplementedError("Base method not implemented")

    def update_job(self, job_status):
        raise NotImplementedError("Base method not implemented")

    def update_events(self, evnt_status):
        raise NotImplementedError("Base method not i    mplemented")

    def get_panda_queue_name(self):
        raise NotImplementedError("Base method not implemented")
