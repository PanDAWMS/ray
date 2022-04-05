import logging
import sys
from time import gmtime

from raythena.utils.config import Config

_initialized = False


def make_logger(config: Config, name: str, filepath: str = None):
    global _initialized
    if not _initialized:
        configure_logger(config, filepath)
        _initialized = True
    return logging.getLogger(name)


def configure_logger(config: Config, filepath: str) -> None:
    """
    Configure the logging format and handlers.

    Args:
        config: application config
        file_logging: if True, write logs to 'config.logging.logfile' in addition to stdout

    Returns:
        None
    """
    log_level = config.logging.get('level', 'warning').upper()
    logging.Formatter.converter = gmtime
    handlers = list()
    if filepath:
        fh = logging.FileHandler(filepath, mode='w')
        handlers.append(fh)
    else:
        ch = logging.StreamHandler(sys.stdout)
        handlers.append(ch)
    if logging.DEBUG == logging.getLevelName(log_level):
        fmt = "{asctime} | {levelname:8} | {name}:{funcName} | {message}"
    else:
        fmt = "{asctime} | {levelname:8} | {name} | {message}"
    logging.basicConfig(
        format=fmt,
        style='{',
        datefmt="%Y-%m-%d %H:%M:%S",
        level=logging.getLevelName(log_level),
        handlers=handlers)
