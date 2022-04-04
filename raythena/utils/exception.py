import threading
from queue import Queue, Empty


class ErrorCodes(object):
    """
    Defines error codes constants and associated default error message for each error code
    """

    PLUGIN_NOT_FOUND = 10
    ILLEGAL_WORKER_STATE = 20
    STAGEIN_FAILED = 30
    STAGEOUT_FAILED = 40
    PAYLOAD_FAILED = 50
    UNKNOWN = 0

    ERROR_CODES_GENRIC_MESSAGES = {
        PLUGIN_NOT_FOUND: "Couldn't import specified plugin",
        ILLEGAL_WORKER_STATE: "Illegal worker state transition",
        STAGEIN_FAILED: "Failed to stagein data",
        STAGEOUT_FAILED: "Failed to stageout data",
        PAYLOAD_FAILED: "Payload execution failed",
        UNKNOWN: "Unknown error"
    }

    @staticmethod
    def get_error_message(error_code: int) -> str:
        """
        Retrieve the error message for a given error code

        Args:
            error_code: error code to lookup

        Returns:
            The default error message
        """
        return ErrorCodes.ERROR_CODES_GENRIC_MESSAGES.get(error_code, "")


class ExThread(threading.Thread):
    """
    Class wrapping thread execution to allow to catch exception occurring during the thread execution.
    Note that this class is overriding Thread.run() to catch exception. Thread's target should therefore be specified
    using the constructor instead of subclassing ExThread as it will override its behaviour
    """

    def __init__(self, *args, **kwargs) -> None:
        """
        Initialize exception queue and pass arguments to the parent constructor

        """
        super().__init__(*args, **kwargs)
        self._ex_queue = Queue()

    def run(self) -> None:
        """
        Wraps the execution of super().run() so that any exception occurring during the execution is caught and added
        to the exception queue to allow a monitoring thread to handle them.

        Returns:
            None
        """
        try:
            super().run()
        except Exception as ex:
            self._ex_queue.put(ex)

    def get_ex_queue(self) -> Queue:
        """
        Retrieve the queue storing exceptions that occurred in the thread

        Returns:
           exceptions queue
        """
        return self._ex_queue

    def exception_occurred(self) -> bool:
        """
        Checks whether or not an exception occurred during the thread execution
        Returns:
            True if an exception occured
        """
        return not self._ex_queue.empty()

    def join_with_ex(self) -> None:
        """
        Tries to join the thread and re-raise any exception that occurred during thread execution

        Returns:
            None

        Raises:
            Any Exception that occurred during the thread execution
        """
        try:
            self.join()
            ex = self._ex_queue.get_nowait()
        except Empty:
            pass
        else:
            if ex:
                raise ex


class BaseRaythenaException(Exception):
    """
    Base class for raythena exception
    """

    def __init__(self, worker_id: str, error_code: int, message: str = None) -> None:
        """
        Initialize worker_id, error code and message

        Args:
            worker_id: worker_id of the node on which the exception occurred
            error_code: error corresponding to the exception type
            message: Optional message overriding the default message associated to the error code
        """
        self.worker_id = worker_id
        self.error_code = error_code
        self.message = message if message else ErrorCodes.get_error_message(
            error_code)
        super().__init__(self.message)

    def __reduce__(self):
        return (self.__class__, (self.worker_id, self.error_code, self.message))


class PluginNotFound(BaseRaythenaException):
    """
    Raised when trying to load a plugin that is not found
    """

    def __init__(self, worker_id: str, message: str = None) -> None:
        super().__init__(worker_id, ErrorCodes.PLUGIN_NOT_FOUND, message)

    def __reduce__(self):
        return (self.__class__, (self.worker_id, self.message))


class IllegalWorkerState(BaseRaythenaException):
    """
    Raised when the worker state tries to transition to a state he shouldn't be able to from its current state.
    """

    def __init__(self, worker_id: str, src_state: str, dst_state: str, message: str = None) -> None:
        super().__init__(worker_id, ErrorCodes.ILLEGAL_WORKER_STATE, message)
        self.src_state = src_state
        self.dst_state = dst_state

    def __reduce__(self):
        return (self.__class__, (self.worker_id, self.src_state, self.dst_state, self.message))


class StageInFailed(BaseRaythenaException):
    """
    Raised when the worker was unable to stage-in data
    """

    def __init__(self, worker_id: str, message: str = None) -> None:
        super().__init__(worker_id, ErrorCodes.STAGEIN_FAILED, message)

    def __reduce__(self):
        return (self.__class__, (self.worker_id, self.message))


class StageOutFailed(BaseRaythenaException):
    """
    Raised when the worker was unable to stage-out data
    """

    def __init__(self, worker_id: str, message: str = None) -> None:
        super().__init__(worker_id, ErrorCodes.STAGEOUT_FAILED, message)

    def __reduce__(self):
        return (self.__class__, (self.worker_id, self.message))


class FailedPayload(BaseRaythenaException):
    """
    Raised when the worker payload failed
    """

    def __init__(self, worker_id: str, message: str = None) -> None:
        super().__init__(worker_id, ErrorCodes.PAYLOAD_FAILED, message)

    def __reduce__(self):
        return (self.__class__, (self.worker_id, self.message))


class UnknownException(BaseRaythenaException):
    """
    Raised when no other exception type applies
    """

    def __init__(self, worker_id: str, message: str = None) -> None:
        super().__init__(worker_id, ErrorCodes.UNKNOWN, message)

    def __reduce__(self):
        return (self.__class__, (self.worker_id, self.message))


class WrappedException(BaseRaythenaException):
    """
    Raised when no other exception type applies
    """

    def __init__(self, worker_id: str, e: Exception) -> None:
        super().__init__(worker_id, ErrorCodes.UNKNOWN, f"Wrapped exception {e}")
        self.wrapped_exception = e

    def __reduce__(self):
        return (self.__class__, (self.worker_id, self.message))


if __name__ == "__main__":
    import time

    def err_func():
        time.sleep(5)
        raise FailedPayload("123")

    t = ExThread(target=err_func)
    t.start()
    try:
        t.join_with_ex()
    except FailedPayload as e:
        print(f"Handling {e}")
    print(t.get_ex_queue().qsize())
