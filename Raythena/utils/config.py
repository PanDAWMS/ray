
import yaml
import os


class Config:

    def __init__(self, *args, **kwargs):

        # parse CLI arguments
        self.config = None
        for k, v in kwargs.items():
            setattr(self, k, v)

        # parse config file
        if self.config is not None and os.path.isfile(self.config):
            with open(self.config) as f:
                file_conf = yaml.safe_load(f)
                for k, v in file_conf.items():
                    if getattr(self, k, None) is None:
                        setattr(self, k, v)
        self._validate()

    def __str__(self):
        return str(self.__dict__)

    def _validate(self):
        pass
