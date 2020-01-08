from abc import ABC, abstractmethod

from raythena.utils.config import Config


class BaseDriver(ABC):
    """
    Defines the interface which should be implemented by raythena drivers.
    """

    def __init__(self, config: Config) -> None:
        """
        Setup config attribute
        Args:
            config: application config
        """
        self.config = config

    @abstractmethod
    def run(self) -> None:
        """
        Start the driver

        Returns:
            None
        """
        raise NotImplementedError("Base driver not implemented")

    @abstractmethod
    def stop(self) -> None:
        """
        Stop the driver

        Returns:
            None
        """
        raise NotImplementedError("Base driver not implemented")
