class BaseDriver():

    def __init__(self, config):
        self.config = config

    def run(self):
        raise NotImplementedError("Base driver not implemented")

    def stop(self):
        raise NotImplementedError("Base driver not implemented")