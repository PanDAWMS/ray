import logging
import time

from raythena.utils.config import Config
from raythena.utils.logging import configure_logger


class Logger(object):
    """
    Actor used to centralize logging from other workers / driver in the same log file.
    """

    def __init__(self, config: Config, id: str) -> None:
        """
        Initialize app config and logger formatting.

        Args:
            config: application config
        """
        self.config = config
        self.id = id
        self.logger = logging.getLogger(self.id)
        configure_logger(self.config, False)

    def debug(self, message: str) -> None:
        """
        Debug log entry
        Args:
            etime:
            actor_id: worker_id of the producer
            message: Message to log
            etime: log entry timestamp

        Returns:
            None
        """
        self.log(logging.DEBUG, message)

    def info(self, message: str) -> None:
        """
        Info log entry
        Args:
            time:
            actor_id: worker_id of the producer
            message: Message to log
            etime: log entry timestamp

        Returns:
            None
        """
        self.log(logging.INFO, message)

    def warning(self, message: str) -> None:
        """
        Warning log entry
        Args:
            actor_id: worker_id of the producer
            message: Message to log
            etime: log entry timestamp

        Returns:
            None
        """
        self.log(logging.WARNING, message)

    def warn(self, message: str) -> None:
        """
        Warning log entry
        Args:
            actor_id: worker_id of the producer
            message: Message to log
            etime: log entry timestamp

        Returns:
            None
        """
        self.log(logging.WARN, message)

    def error(self, message: str) -> None:
        """
        Error log entry
        Args:
            actor_id: worker_id of the producer
            message: Message to log
            etime: log entry timestamp

        Returns:
            None
        """
        self.log(logging.ERROR, message)

    def fatal(self, message: str) -> None:
        """
        Fatal log entry
        Args:
            actor_id: worker_id of the producer
            message: Message to log
            etime: log entry timestamp

        Returns:
            None
        """
        self.log(logging.FATAL, message)

    def critical(self, message: str) -> None:
        """
        Critical log entry
        Args:
            actor_id: worker_id of the producer
            message: Message to log
            etime: log entry timestamp

        Returns:
            None
        """
        self.log(logging.CRITICAL, message)

    def log(self, level: int, message: str) -> None:
        """
        Log the message to file
        Args:
            level: log level
            message: Message to log

        Returns:
            None
        """
        self.logger.log(level, f" {time.asctime()} | {self.id} | {message}")
