

class BaseRaythenaException(Exception):

    def __init__(self, actor_id, error_code=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.actor_id = actor_id
        self.error_code = error_code


class FailedPayload(BaseRaythenaException):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
