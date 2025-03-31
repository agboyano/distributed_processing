import time
import random
import logging
from fsutils import FSDict, FSList, all_lists

def sleep(a, b=None):
    if b is None:
        time.sleep(a)
    else:
        time.sleep(random.uniform(a,b ))


logger = logging.getLogger(__name__)


class FileSystemConnector(object):
    def __init__(self, directory, namespace="tasks", serializer=None):
        self.directory = directory
        self.serializer = serializer
        self.variables = FSDict(directory, "variables", namespace="namespace", serializer=serializer)
        self.namespace = namespace

    def clean_namespace(self):
        "Borra todos los objetos vinculados al namespace"
        self.variables.clear_namespace()

    def get_requests_queue(self, queue_name):
        return f"requests_{queue_name}"

    def get_client_id(self):
        if "nclients" in self.variables:
            nclients = self.variables["nclients"] + 1
        else:
            nclients = 1
        self.variables["nclients"] = nclients
        return f"redis_client_{nclients}"

    def get_server_id(self):
        if "nservers" in self.variables:
            nservers = self.variables["nservers"] + 1
        else:
            nservers = 1 
        self.variables["nservers"] = nservers
        return f"redis_server_{nservers}"

    def get_responses_queue(self, client_id):
        return f"{client_id}_responses"

    def get_reply_to_from_id(self, id_str):
        return id_str.split(":")[0] + "_responses"

    def methods_registry(self):
        """
        Lo usa el cliente.
        Cada método tiene un set de redis con clave {namespace}:method_queues:{method}.
        El contenido del set son los nombres de las colas donde se puden enviar
        los request para elecutar ese método.
        """
        registry = {}

        method_queues = [x for x in self.variables.keys() if "method_queues_" in x]
        
        for method_set in method_queues:
            #method = method_set.split("_")[-1]
            method = method_set.replace("method_queues_", "")
            available = [x for x in self.variables[method_set]]
            registry[method] = available

        return registry

    def register_methods(self, requests_queues_dict):
        """
        Lo usa el servidor para registrar los métodos.
        requests_queues_dict es un diccionario  con el nombre (corto) de las colas
        de clave y un diccionario con los nombres de las funciones de claves y la función de valor
        """
        registry = {}
        for queue_name, func_dict in requests_queues_dict.items():
            for method in func_dict:
                if method in registry:
                    registry[method] += [queue_name]
                else:
                    registry[method] = [queue_name]

        for method in registry:
            method_set = f"method_queues_{method}"
            tmp = self.variables.get(method_set, set())
            self.variables[method_set] = tmp.union(registry[method])
            colas = ", ".join(str(q) for q in registry[method])
            logger.info(f"Method {method} published as available for queues: {colas}")

    def random_queue_for_method(self, method):
        available = self.all_queues_for_method(method)
        if len(available) == 0:
            return None
        return random.choice(available)

    def all_queues_for_method(self, method):
        method_set = f"method_queues_{method}"
        return [x for x in self.variables[method_set]]

    def enqueue(self, queue, msg):
        lst = FSList(directory=self.directory, name=queue, namespace=self.namespace, serializer=self.serializer)
        lst.append(msg)

    def pop(self, queue, timeout=0):
        """
        timeout=0 indefinido
        Lo usa el cliente.
        Devuelve tupla (nombre cola, valor). Si timeout devuelve None
        """
        lst = FSList(directory=self.directory, name=queue, namespace=self.namespace, serializer=self.serializer)
        
        start = time.time()
        if timeout==0:
            timeout=1000

        while (time.time() - start) < timeout:
            try:
                return (queue, lst.pop(0))
            except IndexError:
                sleep(0.1, 0.5)
        return None

    def pop_multiple(self, queues, timeout=0):
        """
        Queues ordenadas por prioridad. 
        Devuelve None si timeout, si no devuelve cola, valor.
        Lo usa el worker.
        """
        lol = [(x, FSList(directory=self.directory, name=x, namespace=self.namespace, serializer=self.serializer)) for x in queues]

        start = time.time()
        if timeout==0:
            timeout=1000

        while (time.time() - start) < timeout:
            for queue, lst in lol:
                try:
                    return (queue, lst.pop(0))
                except IndexError:
                    sleep(0.1, 0.5)
        return None

    def pop_all(self, queue):
        """
        Extrae de la cola y devuelve todos los mensajes disponibles en la cola queue. 
        Lo usa el cliente
        """
        lst = FSList(directory=self.directory, name=queue, namespace=self.namespace, serializer=self.serializer)
        N = len(lst)
        return [lst.pop(0) for i in range(N)]
