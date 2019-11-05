import logging
import os


def configure_logger(config):

    if config.debug:
        log_level = logging.DEBUG
    else:
        log_level = config.logging['level'].upper()

    log_dir = os.path.join(config.logging['logdir'], f"raythena_{os.getpid()}")

    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    log_file = os.path.join(log_dir, 'raythena.log')

    fh = logging.FileHandler(log_file)
    ch = logging.StreamHandler()
    logging.basicConfig(format="{asctime} | {levelname} | {funcName} | {message}",
                        style='{',
                        level=logging.getLevelName(log_level),
                        handlers=[fh, ch])
