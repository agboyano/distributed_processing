import base64
import time
import random
import logging
import dill
from .messages import single_request
from .async_result import AsyncResult

logger = logging.getLogger(__name__)


def serialize_python_call(fn, args=[], kwargs={}):
    pickled_fn = dill.dumps(fn)
    # Decode ascii necesario para que no de error de bytes object no serializable
    return [base64.b64encode(pickled_fn).decode('ascii'), args, kwargs]


class Client():
    def __init__(self, serializer, connector, client_id=None, check_registry="cache",
                 use_reply_to=False, default_queue="default", timeout=24*60*60):
        self.serializer = serializer
        self.connector = connector

        self.use_reply_to = use_reply_to
        
        #Cache for pending responses with id. 
        #ids as keys and time() of message creation as values. 
        self.pending = {}
        #Cache for responses with id.
        #ids as keys and deserialized responses as values.
        self.responses = {}
        #Cache for responses with no id.
        #Deserialized responses as values.
        self.responses_no_id = []
        #Cache for responses that failed to be deserialized.
        self.responses_parse_errors = []
        #Used responses (wait_one_response).
        self.responses_used = set()

        self.check_registry = check_registry
        self.registry = {}

        if check_registry == "cache":
            self.update_registry()

        self.client_id = client_id if client_id is not None else self.connector.get_client_id()
        logger.info(f"Client with id: {self.client_id}")

        self.responses_queue = self.connector.get_responses_queue(self.client_id)
        logger.info(f"Results queue: {self.responses_queue}")

        self.last_request_idnumber = 0
        self.last_request_id = None

        self.set_default_queue(default_queue)

        self.timeout = timeout

    def set_default_queue(self, queue):
        self.default_requests_queue = self.connector.get_requests_queue(queue)

    def generate_id(self):
        self.last_request_idnumber += 1
        self.last_request_id = f"{self.client_id}:{str(self.last_request_idnumber)}"
        return self.last_request_id

    def update_registry(self):
        self.registry = self.connector.methods_registry()

    def select_queue(self, method):
        if self.check_registry == "always":
            requests_queue = self.connector.random_queue_for_method(method)
            if requests_queue is None:
                raise ValueError(f"Method {method} does not exist/is not available.")

        elif self.check_registry == "cache":
            if method not in self.registry:
                self.update_registry()
                if method not in self.registry:
                    raise ValueError(f"Method {method} does not exist/is not available.")

            available = self.registry[method]
            requests_queue = random.choice(available)

        else:
            requests_queue = self.default_requests_queue

        return requests_queue

    def send_single_request(self, method, args=None, kwargs=None, **options):
        """Sends a single request.

        Args:
            method (str): Remote function name.
            args (list): Positional arguments.
            kwargs (dict): Named arguments.
        
        Returns:
            str: id of the request.
        """

        requests_queue = self.select_queue(method)

        id_ = self.generate_id()
        reply_to = None if not self.use_reply_to else self.responses_queue

        sr = single_request(method, args=args, kwargs=kwargs, id=id_,
                            is_notification=False, reply_to=reply_to, **options)
        serialized_sr = self.serializer.dumps(sr)

        self.connector.enqueue(requests_queue, serialized_sr)
        logger.debug(f"Sent request with id: {id_} to queue {requests_queue}")

        self.pending[id_] = time.time()
        return id_

    def all_queues_for_method(self, method):
        if self.check_registry == "always":
            requests_queues = self.connector.all_queues_for_method(method)
            if requests_queues == []:
                raise ValueError(f"Method {method} does not exist/is not available.")

        elif self.check_registry == "cache":
            if method not in self.registry:
                raise ValueError(f"Method {method} does not exist/is not available.")
            requests_queues = self.registry[method]

        else:
            requests_queues = [self.default_requests_queue]

        return requests_queues

    def send_batch_request(self, requests_lst, **options):
        """Sends an batch request that will be executed by a single worker.
    
        Args:
            requests_lst (list): List of tuples [(fname, args, kwargs), ...]
            **options (named args): Opcional args that will added to the rpc
                message in the 'options' key.
        
        Returns:
            list(str): List of ids of the individual sent requests. 
        """

        queues_sets = [set(self.all_queues_for_method(x[0])) for x in requests_lst]

        requests_queues = list(set.union(*queues_sets))

        if len(requests_queues) == 0:
            raise ValueError("No common queue for batch request.")

        requests_queue = random.choice(requests_queues)

        reply_to = None if not self.use_reply_to else self.responses_queue

        batch_request = [single_request(t[0], args=t[1], kwargs=t[2], id=self.generate_id(),
                                        is_notification=False, reply_to=reply_to, **options) for t in requests_lst]

        serialized_br = self.serializer.dumps(batch_request)

        self.connector.enqueue(requests_queue, serialized_br)

        ids = [t["id"] for t in batch_request]
        logger.debug(f"Sent batch request with {len(ids)} requests to {requests_queue}")

        for id in ids:
            self.pending[id] = time.time()

        return ids

    def _responses_to_dicts(self, raw_responses):
        """Deserialize responses.

        Args:
            raw_responses (list): List of responses (serialized), usually from pop or pop_all.

        Returns: 
            (results_dict, no_id, parse_errors)

            results_dict (dict): Dictionary with the ids of the request as keys
                and the deserialized response as value. The deserialized response 
                is a dict with either the key "result" or "error". The get method 
                of the AsyncResult instance, associated with the id, returns the "result", 
                if available, or throws an exception with the information in "error".
                    
            no_id (list): List with all the deserialized responses that have no id.
            
            parse_errors (list): List with all the responses that failed to be deserialized.
        """

        results_dict = {}
        no_id = []
        parse_errors = []

        for e in raw_responses:
            try:
                r = self.serializer.loads(e)

            except:
                parse_errors.append(e)
                continue

            if isinstance(r, list):  # response to batch request
                for rr in r:
                    rr["finished_time"] = time.time()
                    if "id" in rr:
                        results_dict[rr["id"]] = rr
                    else:
                        no_id.append(rr)

            elif isinstance(r, dict):
                r["finished_time"] = time.time()
                if "id" in r:
                    results_dict[r["id"]] = r
                else:
                    no_id.append(r)

        return results_dict, no_id, parse_errors

    def _update_responses_cache(self, raw_responses):
        """Deserialize raw_responses and update caches. 

        Updates the client caches responses, responses_no_id, responses_parse_errors and pending.

        Args:
            raw_responses (list): List of responses (serialized), usually from pop or pop_all.
        """
        responses_dict, no_id, parse_errors = self._responses_to_dicts(raw_responses)
        self.responses.update(responses_dict)
        self.responses_no_id.append(no_id)
        self.responses_parse_errors.append(parse_errors)
        pending = [k for k in self.pending.keys()]
        for id in pending:
            if id in self.responses:
                del self.pending[id]

    def _update_cache_with_all_available_responses(self):
        all_responses = self.connector.pop_all(self.responses_queue)
        self._update_responses_cache(all_responses)

    def wait_responses(self, ids=None, timeout=None):
        """Wait for all responses in ids.

        If ids is None, waits for all pending ids.

        Args:
            ids (list, optional): List of ids. Defaults to None.
            timeout (float, optional): Defaults to None (self.timeout).
                If 0, check queue once.
        
        Returns:
            list: Pending ids if timeout, [] if ok.
       
        Raises:
            TimeoutError
            ValueError: If there are ids neither in responses nor in pending.
        """

        if timeout is None:
            timeout = self.timeout

        if ids is None:
            ids = [k for k in self.pending.keys()]
        else:
            tmp = [k for k in ids if k not in self.responses and k not in self.pending]    
            if len(tmp) > 0:
                raise ValueError(f"wait_responses: {tmp} neither in responses nor in pending.")

        pending = [k for k in ids if k not in self.responses]

        if len(pending)>0:
            self._update_cache_with_all_available_responses() 
        else:
            return []

        pending = [k for k in pending if k not in self.responses]

        if timeout is None:
            forever, time_left = True, -1

        else:
            t_0 = time.time()
            forever, time_left = False, timeout

        while (len(pending)>0) and ((time_left > 0.000001) or forever):
            next_resp = self.connector.pop(self.responses_queue, timeout=time_left)

            if next_resp is not None:  # if None then timeout, if not (queue_name, value)
                self._update_responses_cache([next_resp[1]])

            if not forever:
                time_left = timeout - (time.time() - t_0)

            pending = [k for k in pending if k not in self.responses]

        return pending

    def wait_one_response(self, id, timeout=None, clean=True):
        """Wait for the response with id=id.

        Used by de get method of AsyncResult.

        Args:
            id (str):
            timeout (float, optional): Defaults to None (self.timeout).
                If 0, check queue once.
            clean (bool, optional): If True remove the result from cache.
                Defaults to True. 
        
        Returns:
            dict: Response deserialized) with either the key "result" or "error". 
                The get method of the AsyncResult instance, associated with the id, 
                returns the "result", if available, or throws an exception with the 
                information in "error".

        Raises:
            TimeoutError
            ValueError: If id neither in responses nor in pending.
        """
        if len(self.wait_responses([id], timeout)) > 0:
            raise TimeoutError()

        response = self.responses[id]
        self.responses_used.add(id)
        
        if clean:
            del self.responses[id]

        return response

    def clean_used(self):
        """Clean all responses that have been used at least once.
        """
        responses = [k for k in self.responses]
        for id in responses:
            if id in self.responses_used:
                del self.responses[id]

    def rpc_async(self, method, args=[], kwargs={}):
        """Sends an asynchronous single request.
    
        Args:
            method (str): Remote function name.
            args (list): Positional args.
            kwargs (dict): Named args.
        
        Returns:
            AsyncResult
        """
        id = self.send_single_request(method, args, kwargs)
        return AsyncResult(self, id)

    def rpc_sync(self, method, args=[], kwargs={}, timeout=None):
        """Sends an asynchronous single request.
    
        Args:
            method (str): Remote function name.
            args (list): Positional args.
            kwargs (dict): Named args.
            timeout (float, optional): Defaults to None (self.timeout).
                If 0, check queue once.
            
        Returns:
            result

        Raises:
            TimeoutError
            RemoteException
        """
        return self.rpc_async(method, args, kwargs).get(timeout)

    def rpc_batch_async(self, requests_lst):
        """Sends an asynchronous batch request that will be executed by a single worker.
    
        Args:
            requests_lst (list): List of tuples [(fname, args, kwargs), ...]
        
        Returns:
            list: List of AsyncResult objects
        """
        ids = self.send_batch_request(requests_lst)
        return [AsyncResult(self, id) for id in ids]

    def rpc_batch_sync(self, requests_lst, timeout=None):
        """Sends a synchronous batch request that will be executed by a single worker.
        
        Waits for the results. 
        Uses safe_get, if there's an error in a function, returns None.
        
        Args:
            requests_lst (list): List of tuples [(fname, args, kwargs), ...]
            timeout (float, optional): Defaults to None (self.timeout).
                If 0, check queue once.
            
        Returns:
            list: List of (results or None on error)

        Raises:
            TimeoutError
        """
        fs = self.rpc_batch_async(requests_lst)
        return [f.safe_get(timeout=timeout) for f in fs]
    
    def rpc_multi_async(self, requests_lst):
        """Sends multiple asynchronous requests that will be distributed among workers.
        
        Args:
            requests_lst (list): List of tuples [(fname, args, kwargs), ...].
            
        Returns:
            list: List of AsyncResult objects.
        """
        return [self.rpc_async(t[0], t[1], t[2]) for t in requests_lst]

    def rpc_multi_sync(self, requests_lst, timeout=None):
        """Sends multiple synchronous requests that will be distributed among workers.
        
        Waits for the results. 
        Uses safe_get, if there's an error in a function, returns None.

        Args:
            requests_lst (list): List of tuples [(fname, args, kwargs), ...]
            timeout (float, optional): Defaults to None (self.timeout).
                If 0, check queue once.
            
        Returns:
            list: List of (results or None on error).

        Raises:
            TimeoutError
        """
        fs = self.rpc_multi_async(requests_lst)
        return [f.safe_get(timeout=timeout) for f in fs]

    def rpc_async_fn(self, fn, args=[], kwargs={}):
        """Sends an asynchronous single request with a local python function.
    
        Args:
            fn (function): Local function.
            args (list): Positional args.
            kwargs (dict): Named args.
        
        Returns:
            AsyncResult:
            
        Returns:
            result

        Raises:
            TimeoutError
            RemoteException
        """
        py_call = serialize_python_call(fn, args=args, kwargs=kwargs)
        id = self.send_single_request("eval_py_function", args=py_call)
        return AsyncResult(self, id)

    def rpc_sync_fn(self, method, args=[], kwargs={}, timeout=None):
        """Sends a synchronous single request with a local python function.
    
        Args:
            fn (function): Local function.
            args (list): Positional args.
            kwargs (dict): Named args.
            timeout (float, optional): Defaults to None (self.timeout).
                If 0, check queue once.
            
        Returns:
            result

        Raises:
            TimeoutError
            RemoteException
        """
        return self.rpc_async_fn(method, args, kwargs).get(timeout)
