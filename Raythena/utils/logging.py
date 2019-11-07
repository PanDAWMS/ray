import logging
import os
import sys


def configure_logger(config, file_logging=True):

    if config.logging['level'].lower() == 'debug':
        log_level = logging.DEBUG
    else:
        log_level = config.logging['level'].upper()

    handlers = list()
    ch = logging.StreamHandler(sys.stdout)
    handlers.append(ch)
    if file_logging:
        log_file = os.path.join(os.getcwd(), config.logging['logfile'])
        fh = logging.FileHandler(log_file, mode='w')
        handlers.append(fh)

    logging.basicConfig(format="{asctime} | {levelname} | {name} | {funcName} | {message}",
                        style='{',
                        level=logging.getLevelName(log_level),
                        handlers=handlers)
