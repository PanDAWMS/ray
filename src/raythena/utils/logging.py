import logging
import sys
from time import gmtime

from raythena.utils.config import Config

_initialized = False


def make_logger(config: Config, name: str, filepath: str = None) -> logging.Logger:
    global _initialized
    if not _initialized:
        configure_logger(config, filepath)
        _initialized = True
    return logging.getLogger(name)


def log_to_file(log_level, filepath: str):
    fh = logging.FileHandler(filepath, mode='w')
    fh.setFormatter(logging.Formatter(*get_fmt(log_level)))
    logging.getLogger().addHandler(fh)


def disable_stdout_logging():
    root = logging.getLogger()
    for h in root.handlers:
        if isinstance(h, logging.StreamHandler) and h.stream == sys.stdout:
            root.removeHandler(h)


def get_fmt(log_level):
    if logging.getLevelName(log_level) == logging.DEBUG:
        fmt = "{asctime} | {levelname:8} | {name}:{funcName} | {message}"
    else:
        fmt = "{asctime} | {levelname:8} | {name} | {message}"
    return fmt, "%Y-%m-%d %H:%M:%S", '{'


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
    fmt, datefmt, style = get_fmt(log_level)
    logging.basicConfig(
        format=fmt,
        style=style,
        datefmt=datefmt,
        level=logging.getLevelName(log_level),
        handlers=handlers)
