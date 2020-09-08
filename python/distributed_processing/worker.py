from collections import OrderedDict
import base64
import logging
import time
import dill

from .messages import result_response, error_response, is_single_request, is_batch_request, is_single_response


logger = logging.getLogger(__name__)


def eval_py_function(str_fn, args=[], kwargs={}):
    "str_fn encoded en base64 ascii"
    return dill.loads(base64.b64decode(str_fn))(*args, **kwargs)


class Worker():
    def __init__(self, serializer, connector, server_id=None, with_trace=True, reply_to_default=None):

        self.serializer = serializer
        self.connector = connector

        # El orden de inserción define la prioridad de las colas
        self.requests_queues = OrderedDict()

        # Hay colas que igual preferimos no publicar
        # Por ej. si hay nuchas como en globals()
        self.queues_to_register = set()

        # Si salta una exceción devolvemos la traza remota si with_trace=True
        self.with_trace = with_trace

        # Podemos definir una cola de respuesta por defecto.
        # Sólo se utiliza si el request que recibimos no tiene una clave "reply_to".
        # Si es None obtiene el "reply_to" de la "id" de la request recibida
        self.reply_to_default = reply_to_default

        self.server_id = server_id if server_id is not None else self.connector.get_server_id()

        logging.info(f"Worker id: {self.server_id}")

    def add_requests_queue(self, name, func_dict, register=True):
        """
        register sólo hace referena a la publicacion
        las funciones iguen estando disponibles
        Hay colas que puede que no quiera hacer públicas por ej. porque tiene muchos métodos
        """

        requests_queue = self.connector.get_requests_queue(name)
        self.requests_queues[requests_queue] = func_dict
        if register:
            self.queues_to_register.add(requests_queue)
        # no olvidar self.update_registry()

    def add_function(self, queue, fn_name, fn):
        requests_queue = self.connector.get_requests_queue(queue)
        self.requests_queues[requests_queue][fn_name] = fn
        # no olvidar self.update_registry()

    def add_requests_queues(self, queues):
        for name in queues:
            self.add_requests_queue(name, queues[name])
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
            logger.debug(f"Recieved single request in queue {request_queue}")
            return self._process_single_request(request, request_queue)

        elif is_batch_request(request):
            logger.debug(f"Recieved single request in queue {request_queue}")
            if len(request) == 0:
                return error_response(-32600)

            responses = [self._process_single_request(rq, request_queue) for rq in request]
            response = [r for r in responses if r is not None]

            if len(response) > 0:
                return response

        else:
            return error_response(-32600)

    def get_reply_to(self, request):
        """
        Podemos definir una cola de respuesta por defecto.
        Sólo se utiliza si el request que recibimos no tiene una clave "reply_to".
        Si es None obtiene el "reply_to" de la "id" de la request recibida
        """
        if "reply_to" in request and request["reply_to"] is not None:
            return request["reply_to"]

        if self.reply_to_default is not None:
            return self.reply_to_default

        if "id" in request:
            try:
                #reply_to = self.connector.get_reply_to_from_id(request["id"])
                id_str = request["id"]
                reply_to = ":".join(id_str.split(":")[:-1]) + ":responses"
                return reply_to
            except:
                return None

        return None

    def _process_single_request(self, request, request_queue):
        """
        Devuelve una tupla con donde el primer elemento es la respuesta serializada (error o result) o None,
        la segunda la id o None y la tercera reply_to o None
        """

        if not is_single_request(request):
            return error_response(-32600)

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

    def run_once(self, timeout=0):

        sorted_queues = self.requests_queues.keys()

        if len(sorted_queues) == 0:
            raise ValueError("No queues to listen.")

        #pop_multiple devuelve el nombre de la cola y el resultadado o None si timeout
        request_with_priority = self.connector.pop_multiple(sorted_queues, timeout=timeout)

        if request_with_priority is not None:
            request_queue, serialized_request = request_with_priority
            processed = self.process_request(serialized_request, request_queue)

            if is_single_response(processed):
                processed = [processed]

            for one_single_response in processed:

                id_ = one_single_response.get("id", None)
                reply_to = one_single_response.get("reply_to", None)

                logger.debug(f"Processed request with id {id_} in queue {request_queue}")

                if not one_single_response["is_notification"] and reply_to is not None:
                    self.connector.enqueue(reply_to, self.serializer.dumps(one_single_response))
                    logger.debug(f"Response with id {id_} sent to {reply_to}")

                else:
                    logger.debug(f"Response with id {id_} processed but not sent.")

        else:
            logger.debug(f"run_once timeout")

    def run(self, timeout=None):
        if timeout is None or timeout <= 0:
            forever = True
            timeout = 0
        else:
            t_0 = time.time()
            forever = False
            time_left = timeout

        while forever or time_left > 0:
            self.run_once(timeout=timeout)
            if not forever:
                time_left = timeout - (time.time() - t_0)
