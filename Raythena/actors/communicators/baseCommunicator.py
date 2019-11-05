
class BaseCommunicator:

    def __init__(self, actor, config):
        self.actor = actor
        self.config = config

    def start(self):
        raise NotImplementedError("Base method not implemented")

    def stop(self):
        raise NotImplementedError("Base method not implemented")

    def fix_command(self, command):
        raise NotImplementedError("Base method not implemented")
