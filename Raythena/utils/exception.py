class ErrorCodes:

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
    def get_error_message(error_code):
        return ErrorCodes.ERROR_CODES_GENRIC_MESSAGES.get(error_code, "")


class BaseRaythenaException(Exception):

    def __init__(self, id, error_code, message=None):
        self.id = id
        self.error_code = error_code
        self.message = message if message else ErrorCodes.get_error_message(error_code)


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
