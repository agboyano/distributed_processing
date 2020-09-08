from .exceptions import RemoteException


PENDING = 'PENDING'
FINISHED = 'FINISHED'
FAILED = 'FAILED'


class AsyncResult(object):

    def __init__(self, rpc_client, id):
        self._client = rpc_client
        self.id = id
        self.status = PENDING
        self.result = None
        self.error = None

    def finished(self):
        return self.status == FINISHED

    def failed(self):
        return self.status == FAILED

    def pending(self):
        return self.status == PENDING

    def _raise_exception(self, error):
        raise RemoteException(error)

    def get(self, timeout=None, clean=True):
        if self.finished():
            return self.result
        elif self.failed():
            self._raise_exception(self.error)

        response = self._client.wait_response(self.id, timeout, clean=clean)

        if "result" in response:
            self.status = FINISHED
            self.result = response["result"]
            return self.result

        elif "error" in response:
            self.status = FAILED
            self.error = response["error"]
            self._raise_exception(self.error)

    def safe_get(self, timeout=None, clean=True, default=None):
        try:
            return self.get(timeout, clean=clean)
        except:
            return default
