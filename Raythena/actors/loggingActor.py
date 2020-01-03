import logging

import ray

from Raythena.utils.config import Config
from Raythena.utils.logging import configure_logger


@ray.remote(num_cpus=0)
class LoggingActor:

    def __init__(self, config: Config) -> None:
        self.config = config
        self.logger = logging.getLogger()
        configure_logger(self.config)

    def debug(self, actor_id: str, message: str) -> None:
        self.log(logging.DEBUG, actor_id, message)

    def info(self, actor_id: str, message: str) -> None:
        self.log(logging.INFO, actor_id, message)

    def warning(self, actor_id: str, message: str) -> None:
        self.log(logging.WARNING, actor_id, message)

    def warn(self, actor_id: str, message: str) -> None:
        self.log(logging.WARN, actor_id, message)

    def error(self, actor_id: str, message: str) -> None:
        self.log(logging.ERROR, actor_id, message)

    def fatal(self, actor_id: str, message: str) -> None:
        self.log(logging.FATAL, actor_id, message)

    def critical(self, actor_id: str, message: str) -> None:
        self.log(logging.CRITICAL, actor_id, message)

    def log(self, level, actor_id: str, message: str) -> None:
        self.logger.log(level, f"{actor_id} | {message}")
