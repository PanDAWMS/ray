import logging
import os
import sys
from Raythena.utils.config import Config


def configure_logger(config: Config, file_logging: bool = True) -> None:

    if config.logging['level'].lower() == 'debug':
        log_level = logging.DEBUG
    else:
        log_level = config.logging['level'].upper()

    handlers = list()
    ch = logging.StreamHandler(sys.stdout)
    handlers.append(ch)
    if file_logging:
        logdir = os.path.expandvars(config.ray.get('workdir', os.getcwd()))
        if not os.path.isdir(logdir):
            logdir = os.getcwd()
        log_file = os.path.join(logdir, config.logging['logfile'])
        fh = logging.FileHandler(log_file, mode='w')
        handlers.append(fh)

    logging.basicConfig(
        format="{asctime} | {levelname} | {name} | {funcName} | {message}",
        style='{',
        level=logging.getLevelName(log_level),
        handlers=handlers)
