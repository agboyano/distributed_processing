from time import time
from .exceptions import RemoteException


PENDING = 'PENDING'
OK = 'OK'
FAILED = 'FAILED'
CLEANED = 'CLEANED'

class AsyncResult(object):

    def __init__(self, rpc_client, id):
        self._client = rpc_client
        self.id = id
        self._status = PENDING
        self.value = None
        self.error = None
        self.creation_time = time()
        self.finished_time = None

    def ok(self):
        return self.status == OK

    def failed(self):
        return self.status == FAILED
    
    def pending(self):
        """Returns True if the state of the AsyncResult object is 'PENDING'.

        Syncs the object with rpc_client, just in case we have used wait_responses 
        from the client or if there are responses available in the client queue.

        'PENDING' state should be assumed as transitory.

        Returns:
            bool: True if 'PENDING', False otherwise   
        """
        return self.status == PENDING
    
    def _raise_exception(self, error):
        """Raises a RemoteException with the information in error.

        Args:
            error (dict): Dictionary with "code", "message" and/or "trace" as keys 
                and a str as value.

        Raises:
            RemoteException  
        """
        raise RemoteException(error)
    
    @property
    def status(self):
        """Returns the status of the AsyncResult object.

        Syncs the object with rpc_client, just in case we have used wait_responses 
        from the client or if there are responses available in the client queue.

        'PENDING' state should be assumed as transitory. 

        Returns:
            str: 'PENDING', 'OK' or 'FAILED'   
        """
        if self._status == PENDING:
            try:
                self.wait(timeout=0) 
            except TimeoutError:
                pass
        return self._status

    def get(self, timeout=None, clean=True):
        """Returns the value of the AsyncResult object.
        
        Throws a RemoteException exception with the information 
        in "error" of the response message.
        
        Args:
            timeout (float, optional): Defaults to None (rpc_client.timeout).
                If 0, check queue once.
            clean (bool, optional): If True remove the result from cache.
                Defaults to True. 
        
        Returns:
            result

        Raises:
            TimeoutError
            RemoteException
        """      
        self.wait(timeout, clean)
        if self.ok():
            return self.value
        elif self.failed():
            self._raise_exception(self.error)
        raise ValueError("AsyncResult: Undefined Value.") # shouldn`t happen

    def wait(self, timeout=None, clean=True):
        """Waits for result and updates the AsyncResult object.
       
        Throws TimeoutError if timeout reached.

        Args:
            timeout (float, optional): Defaults to None (rpc_client.timeout).
                If 0, check queue once.
            clean (bool, optional): If True remove the result from cache.
                Defaults to True. 
      
        Raises:
            TimeoutError
        """
        if self._status == PENDING:
            response = self._client.wait_one_response(self.id, timeout, clean=clean)
            if "result" in response:
                self.finished_time = response["finished_time"]
                self._status = OK
                self.value = response["result"]
            elif "error" in response:
                self.finished_time = response["finished_time"]
                self._status = FAILED
                self.error = response["error"]


    def safe_get(self, timeout=None, clean=True, default=None):
        try:
            return self.get(timeout, clean=clean)
        except:
            return default
        
def gather(async_result_lst):
    pass
