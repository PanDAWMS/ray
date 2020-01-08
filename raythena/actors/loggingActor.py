import logging

import ray

from raythena.utils.config import Config
from raythena.utils.logging import configure_logger


@ray.remote(num_cpus=0)
class LoggingActor(object):
    """
    Actor used to centralize logging from other workers / driver in the same log file.
    """

    def __init__(self, config: Config) -> None:
        """
        Initialize app config and logger formatting.

        Args:
            config: application config
        """
        self.config = config
        self.logger = logging.getLogger()
        configure_logger(self.config)

    def debug(self, actor_id: str, message: str) -> None:
        """
        Debug log entry
        Args:
            actor_id: worker_id of the producer
            message: Message to log

        Returns:
            None
        """
        self.log(logging.DEBUG, actor_id, message)

    def info(self, actor_id: str, message: str) -> None:
        """
        Info log entry
        Args:
            actor_id: worker_id of the producer
            message: Message to log

        Returns:
            None
        """
        self.log(logging.INFO, actor_id, message)

    def warning(self, actor_id: str, message: str) -> None:
        """
        Warning log entry
        Args:
            actor_id: worker_id of the producer
            message: Message to log

        Returns:
            None
        """
        self.log(logging.WARNING, actor_id, message)

    def warn(self, actor_id: str, message: str) -> None:
        """
        Warning log entry
        Args:
            actor_id: worker_id of the producer
            message: Message to log

        Returns:
            None
        """
        self.log(logging.WARN, actor_id, message)

    def error(self, actor_id: str, message: str) -> None:
        """
        Error log entry
        Args:
            actor_id: worker_id of the producer
            message: Message to log

        Returns:
            None
        """
        self.log(logging.ERROR, actor_id, message)

    def fatal(self, actor_id: str, message: str) -> None:
        """
        Fatal log entry
        Args:
            actor_id: worker_id of the producer
            message: Message to log

        Returns:
            None
        """
        self.log(logging.FATAL, actor_id, message)

    def critical(self, actor_id: str, message: str) -> None:
        """
        Critical log entry
        Args:
            actor_id: worker_id of the producer
            message: Message to log

        Returns:
            None
        """
        self.log(logging.CRITICAL, actor_id, message)

    def log(self, level, actor_id: str, message: str) -> None:
        """
        Log the message to file
        Args:
            level: log level
            actor_id: worker_id of the producer
            message: Message to log

        Returns:
            None
        """
        self.logger.log(level, f"{actor_id} | {message}")
