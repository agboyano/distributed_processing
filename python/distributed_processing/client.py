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

        self.pending = {}

        self.responses = {}
        self.responses_no_id = []
        self.responses_parse_errors = []

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
        "espera una lista de tuplas (fname, args, kwargs)"

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

    def _responses_to_dicts(self, json_lst):
        results_dict = {}
        no_id = []
        parse_errors = []

        for e in json_lst:
            try:
                r = self.serializer.loads(e)

            except:
                parse_errors.append(e)
                continue

            if isinstance(r, list):  # respuesta a un batch request
                for rr in r:
                    if "id" in rr:
                        results_dict[rr["id"]] = rr
                    else:
                        no_id.append(rr)

            elif isinstance(r, dict):
                if "id" in r:
                    results_dict[r["id"]] = r
                else:
                    no_id.append(r)

        return results_dict, no_id, parse_errors

    def _update_responses_cache(self, responses):
        responses_dict, no_id, parse_errors = self._responses_to_dicts(responses)
        self.responses.update(responses_dict)
        self.responses_no_id.append(no_id)
        self.responses_parse_errors.append(parse_errors)

    def _fetch_all_available_responses(self):
        all_responses = self.connector.pop_all(self.responses_queue)
        self._update_responses_cache(all_responses)

    def _clean_response(self, id):
        del self.responses[id]

    def _get_response(self, id, clean=True):
        if id in self.responses:
            val = self.responses[id]
            del self.pending[id]
            if clean:
                self._clean_response(id)
            return (True, val)
        else:
            return (False, None)

    def wait_response(self, id, timeout=None, clean=True):

        if timeout is None:
            timeout = self.timeout

        if id not in self.responses:
            self._fetch_all_available_responses()

        available, response = self._get_response(id, clean=clean)

        if available:
            return response

        if timeout is None:
            forever = True
            timeout = 0

        else:
            t_0 = time.time()
            forever = False
            time_left = timeout

        while forever or time_left > 0:

            next_resp = self.connector.pop(self.responses_queue, timeout=time_left)

            if next_resp is not None:  # si es None es por Timeout
                self._update_responses_cache([next_resp[1]])

            available, response = self._get_response(id, clean=clean)
            if available:
                return response

            if not forever:
                time_left = timeout - (time.time() - t_0)

        raise TimeoutError()

    def rpc_async(self, method, args=[], kwargs={}):
        id = self.send_single_request(method, args, kwargs)
        return AsyncResult(self, id)

    def rpc_sync(self, method, args=[], kwargs={}, timeout=None):
        return self.rpc_async(method, args, kwargs).get(timeout)

    def rpc_batch_async(self, requests_lst):
        "espera una lista de tuplas (fname, args, kwargs)"
        ids = self.send_batch_request(requests_lst)
        return [AsyncResult(self, id) for id in ids]

    def rpc_batch_sync(self, requests_lst, timeout=None):
        "espera una lista de tuplas (fname, args, kwargs)"
        fs = self.rpc_batch_async(requests_lst)
        return [f.get(timeout=timeout) for f in fs]

    def rpc_async_fn(self, fn, args=[], kwargs={}):
        py_call = serialize_python_call(fn, args=args, kwargs=kwargs)
        id = self.send_single_request("eval_py_function", args=py_call)
        return AsyncResult(self, id)

    def rpc_sync_fn(self, method, args=[], kwargs={}, timeout=None):
        return self.rpc_async_fn(method, args, kwargs).get(timeout)
