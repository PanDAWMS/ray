import threading
from queue import Queue, Empty


class ErrorCodes(object):
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
        return ErrorCodes.ERROR_CODES_GENRIC_MESSAGES.get(error_code, "")


class ExThread(threading.Thread):

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.__ex_queue = Queue()

    def run(self):
        try:
            super().run()
        except Exception as ex:
            self.__ex_queue.put(ex)

    def get_ex_queue(self) -> Queue:
        return self.__ex_queue

    def exception_occurred(self) -> bool:
        return not self.__ex_queue.empty()

    def join_with_ex(self):
        try:
            self.join()
            ex = self.__ex_queue.get_nowait()
        except Empty:
            pass
        else:
            if ex:
                raise ex


class BaseRaythenaException(Exception):

    def __init__(self, id, error_code, message=None):
        self.id = id
        self.error_code = error_code
        self.message = message if message else ErrorCodes.get_error_message(
            error_code)


class PluginNotFound(BaseRaythenaException):

    def __init__(self, id, message=None):
        super().__init__(id, ErrorCodes.PLUGIN_NOT_FOUND, message)


class IllegalWorkerState(BaseRaythenaException):

    def __init__(self, id, src_state, dst_state, message=None):
        super().__init__(id, ErrorCodes.ILLEGAL_WORKER_STATE, message)
        self.src_state = src_state
        self.dst_state = dst_state


class StageInFailed(BaseRaythenaException):

    def __init__(self, id, message=None):
        super().__init__(id, ErrorCodes.STAGEIN_FAILED, message)


class StageOutFailed(BaseRaythenaException):

    def __init__(self, id, message=None):
        super().__init__(id, ErrorCodes.STAGEOUT_FAILED, message)


class FailedPayload(BaseRaythenaException):

    def __init__(self, id, message=None):
        super().__init__(id, ErrorCodes.PAYLOAD_FAILED, message)


class UnknownException(BaseRaythenaException):

    def __init__(self, id, message=None):
        super().__init__(id, ErrorCodes.UNKNOWN, message)


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
