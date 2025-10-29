from collections import OrderedDict
import base64
import logging
import time
import dill

from .messages import result_response, error_response, is_single_request, is_batch_request, is_single_response, is_batch_response


logger = logging.getLogger(__name__)
#logger.setLevel(logging.DEBUG)

def eval_py_function(str_fn, args=[], kwargs={}):
    "str_fn encoded en base64 ascii"
    return dill.loads(base64.b64decode(str_fn))(*args, **kwargs)


class Worker():
    def __init__(self, serializer, connector, server_id=None, with_trace=True, reply_to_default=None):
        self.serializer = serializer
        self.connector = connector

        # El orden de inserción define la prioridad de las colas
        # OrderedDict no longer necessary. Dicts respect key insertion 
        # order since Python 3.7.
        self.requests_queues = OrderedDict()

        # Hay colas que igual preferimos no publicar
        # Por ej. si hay nuchas como en globals()
        self.queues_to_register = set()

        # Si salta una excepción devolvemos la traza remota si with_trace=True
        self.with_trace = with_trace

        # Podemos definir una cola de respuesta por defecto.
        # Sólo se utiliza si el request que recibimos no tiene una clave "reply_to".
        # Si es None obtiene el "reply_to" de la "id" de la request recibida
        self.reply_to_default = reply_to_default
        self.server_id = server_id if server_id is not None else self.connector.get_server_id()
        logger.info(f"Worker id: {self.server_id}")

    def add_requests_queue(self, simple_queue_name, func_dict, register=True):
        """Add queue with functions.

        If `register` is True, the functions and their queues are made public
        with the Worker's `update_registry` method. 

        Args:
            simple_queue_name (str): Simple name for the queue where requests
                will be received. The complete name of the queue depends on wether 
                the queue is a `requests` or a `responses` queue, and is 
                obtained using the `get_requests_queue(simple_queue_name)` and 
                `get_responses_queue(simple_queue_name)` respectively from the 
                Connector's instance. In this case, 
                `get_requests_queue(simple_queue_name)` will be called.     
            fun_dict (dict): Dictionary with public method names as keys
                and functions as values {"func_name":func, ...}.
            register (bool): Defaults to True. If False, functions (methods) 
                and their queues are not made public, but remain available
                anonymously.This prevents exposing queues that may contain 
                many methods or should remain private.

        """
        # Generate internal queue name requests_{queue_name} from queue_name
        requests_queue = self.connector.get_requests_queue(simple_queue_name)
        self.requests_queues[requests_queue] = func_dict
        if register:
            self.queues_to_register.add(requests_queue)
        # no olvidar self.update_registry()

    def add_function(self, simple_queue_name, fn_name, fn, register=True):
        """Add function to a queue.

        If `register` is True, the function and the queue are made public
        with the Worker's `update_registry` method. 

        Args:
            simple_queue_name (str): Simple name for the queue where requests
                will be received. The complete name of the queue depends on wether 
                the queue is a `requests` or a `responses` queue, and is 
                obtained using the `get_requests_queue(simple_queue_name)` and 
                `get_responses_queue(simple_queue_name)` respectively from the 
                Connector's instance. In this case, 
                `get_requests_queue(simple_queue_name)` will be called.  
            fn_name (str): Method name to be available for the queue.
            register (bool): Defaults to True. If False, the method 
                and the queue are not made public, but remain available
                anonymously.

        """

        requests_queue = self.connector.get_requests_queue(simple_queue_name)

        if requests_queue not in self.requests_queues:
            self.add_requests_queue(simple_queue_name, {fn_name: fn}, register)
        else:
            self.requests_queues[requests_queue][fn_name] = fn
        # no olvidar self.update_registry()

    def add_python_eval(self, simple_queue_name="py_eval", register=True):
        """Adds a queue with the method `eval_py_function`, that evals a serialized python function.

        If `register` is True, `eval_py_function` and the queue are made public
        with the Worker's `update_registry` method. 

        Equivalent to:
        
        import dill
        import base64

        def eval_py_function(str_fn, args=[], kwargs={}):
            "str_fn encoded en base64 ascii"
            return dill.loads(base64.b64decode(str_fn))(*args, **kwargs)

        server.add_requests_queue("py_eval", {"eval_py_function": eval_py_function})
        
        Args:
            simple_queue_name (str): Simple name for the queue where requests
                will be received. Defaults to 'py_eval'.
            register (bool): Defaults to True. If False, `eval_py_function` 
                and the queue are not made public, but remain available
                anonymously. 

        """
        self.add_function(simple_queue_name, "eval_py_function", eval_py_function, register)

    def add_requests_queues(self, queues, register=True):
        """Add the information in the queues dict to the registry.
        
        If `register` is True, the functions and their queues are made public
        with the Worker's `update_registry` method. 
        
        Args:
            queues (dict): Dictionary with the same structure as the Worker's
                requests_queues attribute. Unlike the dictionary in the 
                Worker's requests_queues attribute, the keys in queues 
                represent the simple names of the queues rather than 
                their long names. The corresponding long name for each queue 
                is retrieved using get_requests_queue(simple_queue_name).
                Structure:
                    {simple_queue_name:{method_name:function, ...}, ...}
            register (bool): Defaults to True. If False, functions (methods) 
                and their queues are not made public, but remain available
                anonymously.

        """
        for simple_queue_name in queues:
            self.add_requests_queue(simple_queue_name, queues[simple_queue_name], register=register)
        # no olvidar self.update_registry()

    def update_methods_registry(self):
        queues_to_register = {k: v for (k, v) in self.requests_queues.items()
                              if k in self.queues_to_register}

        self.connector.register_methods(queues_to_register)

    def process_request(self, msg, request_queue):
        try:
            request = self.serializer.loads(msg)

        except:
            return error_response(-32700, with_trace=self.with_trace)

        if is_single_request(request):
            logger.debug(f"Received Single Request from queue: {request_queue}")
            return self._process_single_request(request, request_queue)

        elif is_batch_request(request):
            # A Batch Request generate a Batch Response.
            # Should consider whether it makes sense to allow sending 
            # individual responses as soon as they are available.
            logger.debug(f"Received Batch Request with {len(request)} requests from queue: {request_queue}")
            if len(request) == 0:
                return error_response(-32600)

            responses = [self._process_single_request(rq, request_queue) for rq in request]
            response = [r for r in responses if r is not None]

            if len(response) > 0:
                return response

        else:
            return error_response(-32600)

    def get_reply_to_from_id(self, id):
        """Returns responses queue name from requests id.

        Args:
            id (str): Request Message id. With format "{client_id}:num".
        
        Returns:
            str: Queue name. "{client_id}_responses".

        """
        client_id = id.strip().split(":")[0]
        return self.connector.get_responses_queue(client_id)


    def get_reply_to(self, request):
        """Returns the queue name where response is going to be sent.

        Returns the content of the key "reply_to" in request, if it is
        not None. If it is None, or the key "reply_to" does not exists, 
        returns the content of the attribute "reply_to_default"
        of the Worker instance.
        
        If the above fails, generates the queue name from the key "id"
        of request, assuming format "{client_id}:num".
        
        Otherwise None.
        
        Note: The JSON RPC 2.0 specification does not have a "reply_to" attribute
        in the message object. If we want to be compliant we should able to respond
        to requests that have not defined "reply_to".  

        The specification also says that "If there was an error in detecting the id 
        in the Request object (e.g. Parse error/Invalid Request), it MUST be Null."

        Args:
            request (dict)
        
        Returns:
            str: The queue name to send the response or None.
    
        """
        if "reply_to" in request and request["reply_to"] is not None:
            return request["reply_to"]

        if self.reply_to_default is not None:
            return self.reply_to_default

        if "id" in request:
            try:
                return self.connector.get_reply_to_from_id(request["id"])
            except:
                return None

        return None

    def _process_single_request(self, request, request_queue):
        """Process a single request.

        Args:
            request (dict): Deserialized request
            request_queue (str): Queue name where the method to be used is published.

        Returns:
            dict: result_response or error_response (from messages)

        """


        if not is_single_request(request):
            return error_response(-32600)
        
        # I should check if is_notification is necessary in a response.
        # Doesn't make any sense.
        is_notification = request["is_notification"]
        id_ = request.get("id", None)
        reply_to = self.get_reply_to(request)

        if not is_single_request(request):
            return error_response(-32600, id=id_, reply_to=reply_to,
                                  is_notification=is_notification)

        if request["method"] not in self.requests_queues[request_queue]:
            return error_response(-32601, id=id_, reply_to=reply_to,
                                  is_notification=is_notification)

        args = request["args"] if "args" in request else []
        kwargs = request["kwargs"] if "kwargs" in request else {}

        try:
            func = self.requests_queues[request_queue]
            result = func[request["method"]](*args, **kwargs)

            return result_response(result, id=id_, reply_to=reply_to,
                                   is_notification=is_notification)

        except TypeError:
            return error_response(-32602, id=id_, reply_to=reply_to,
                                  is_notification=is_notification, with_trace=self.with_trace)

        except:
            return error_response(-32603, id=id_, reply_to=reply_to,
                                  is_notification=is_notification, with_trace=self.with_trace)

    def run_once(self, timeout=-1):
        sorted_queues = self.requests_queues.keys()

        if len(sorted_queues) == 0:
            raise ValueError("No queues to listen.")

        # `pop_multiple` returns tuple (queue name, serialized_request), or None, if timeout
        # returns only ONE request (can be a Batch Request)
        request_with_priority = self.connector.pop_multiple(sorted_queues, timeout=timeout)

        if request_with_priority is not None:
            request_queue, serialized_request = request_with_priority
            # Should split deserialization and computation to make easier
            # to implement a parallel computation of Batch Requests in the future.
            processed = self.process_request(serialized_request, request_queue)

            if is_single_response(processed):
                id_ = processed.get("id", None)
                if id_ is not None:
                    reply_to = processed.get("reply_to", None)
                    self.connector.enqueue(reply_to, self.serializer.dumps(processed))
                    logger.debug(f"Sent Single Response with id {id_} to queue {reply_to}")
                else:
                    logger.debug(f"Processed Notification from queue {request_queue}")

            elif is_batch_response(processed):
                # Generates one Batch Response for each queue in reply_to.
                # Each request within the Batch Request could have a different `reply_to`. 
                # 
                # Could implement a parallel computation of Batch Requests in the future. 
                batch = {}
                for one_single_response in processed:
                    id_ = one_single_response.get("id", None)
                    reply_to = one_single_response.get("reply_to", None)
                    logger.debug(f"Processed Request with id {id_} from Batch Request from queue {request_queue}")

                    if not one_single_response["is_notification"] and reply_to is not None:
                        if reply_to not in batch:
                            batch[reply_to] = [one_single_response]                            
                        else:
                            batch[reply_to].append(one_single_response)

                        logger.debug(f"Response with id {id_} added to Batch Response for queue {reply_to}")

                    else:
                        logger.debug(f"Notification with id {id_} processed but not sent.")
                
                for reply_queue in batch:
                    self.connector.enqueue(reply_queue, self.serializer.dumps(batch[reply_queue]))
                    logger.debug(f"Sent Batch Response with {len(batch[reply_queue])} items to queue {reply_queue}")

        else:
            logger.debug(f"run_once timeout")

    def run(self, timeout=None):
        if timeout is None or timeout <= -0.00001:
            forever = True
            timeout = -1.0 # forever
        else:
            t_0 = time.time()
            forever = False
            time_left = timeout

        while forever or time_left > 0:
            self.run_once(timeout=timeout)
            if not forever:
                time_left = timeout - (time.time() - t_0)
