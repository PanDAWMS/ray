

class BaseRaythenaException(Exception):

    def __init__(self, id, error_code=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.id = id
        self.error_code = error_code


class PluginNotFound(BaseRaythenaException):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class IllegalWorkerState(BaseRaythenaException):

    def __init__(self, src_state, dst_state, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.src_state = src_state
        self.dst_state = dst_state


class StageInFailed(BaseRaythenaException):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class StageOutFailed(BaseRaythenaException):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class FailedPayload(BaseRaythenaException):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class UnknownException(BaseRaythenaException):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
