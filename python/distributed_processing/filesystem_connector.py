import time
import random
import logging
import fsutils

def sleep(a, b=None):
    if b is None:
        time.sleep(a)
    else:
        time.sleep(random.uniform(a, b))

logger = logging.getLogger(__name__)

class FileSystemConnector(object):
    def __init__(self, base_path, temp_dir=None, serializer=fsutils.structs.joblib_serializer):
        self.namespace = fsutils.structs.FSNamespace(base_path, temp_dir, serializer)
        self.variables = self.namespace.udict("variables")
        self.with_watchdog = True
        self.ntries_register_lock = 5
        self.pop_sleep = (5, 10) # sólo si with_watchdog es False
        self.pop_sleep_watchdog = (0.0, 0.5)
        self.pop_timeout = 60
        self.registry_timeout = 10

    def clean_namespace(self):
        "Borra todos los objetos vinculados al namespace"
        self.namespace.clear()
        self.variables = self.namespace.udict("variables")

    def get_requests_queue(self, queue_name):
        return f"requests_{queue_name}"

    def get_client_id(self):
        """
        Cuidado. No atómico.
        """
        if "nclients_set" not in self.variables:
            self.variables["nclients_set"] = True
            self.variables["nclients"] = 1
            nclients = 1
            return f"fs_client_{nclients}"
        
        ntries = 0
        while ntries < 5:
            try:
                nclients = self.variables.pop("nclients")
                self.variables["nclients"] = nclients + 1
                return f"fs_client_{nclients}"
            except KeyError:
                pass
            ntries += 1
            sleep(0.1, 1.0)
    
    def get_server_id(self):
        """
        Cuidado. No atómico.
        """
        if "nservers_set" not in self.variables:
            self.variables["nservers_set"] = True
            self.variables["nservers"] = 1
            nservers = 1
            return f"fs_client_{nservers}"

        ntries = 0
        while ntries < 5:
            try:
                nservers = self.variables.pop("nservers")
                self.variables["nservers"] = nservers + 1
                return f"fs_client_{nservers}"
            except KeyError:
                pass
            ntries += 1
            sleep(0.1, 1.0)        

    def get_responses_queue(self, client_id):
        return f"{client_id}_responses"

    def get_reply_to_from_id(self, id_str):
        return id_str.split(":")[0] + "_responses"
    
    def wait_until_registry_lock_release(self, ntries):
        for i in range(ntries):
            if "registry_lock" not in self.variables:
                return
            e, _, _, _ = fsutils.watchdog.wait_until_file_event([self.variables.base_path], ["registry_lock"], ["deleted", "moved"], timeout=self.registry_timeout)
            if e is not None:
                    return
        logger.warning(f"registry_lock not released: tried {i + 1} times")

    def lock_registry(self, ntries=5):
        for i in range(ntries):
            if "registry_lock" not in self.variables:
                self.variables["registry_lock"] = True
                logger.debug(f"registry_lock locked: tried {i + 1} times")
                return
            self.wait_until_registry_lock_release(5)
            if i < ntries:
                sleep(0.1, 0.5)
        self.variables["registry_lock"] = True
        logger.warning(f"registry_lock set anyway: tried {i + 1} times")

    def unlock_registry(self):
        del self.variables["registry_lock"]
        logger.debug("registry_lock unlocked")

    def methods_registry(self):
        """
        Lo usa el cliente.
        Cada método tiene un set de redis con clave {namespace}:method_queues:{method}.
        El contenido del set son los nombres de las colas donde se pueden enviar
        los request para ejecutar ese método.
        """
        registry = {}
        self.lock_registry(self.ntries_register_lock)
        try:
            method_queues = [x for x in self.variables.keys() if "method_queues_" in x]
        finally:    
            self.unlock_registry()
        
        for method_set in method_queues:
            method = method_set.replace("method_queues_", "")
            available = [x for x in self.variables[method_set]]
            registry[method] = available

        return registry

    def register_methods(self, requests_queues_dict):
        """
        Lo usan los workers para hacer públicas las colas FIFO en las que están disponibles los métodos (funciones).
        El cliente puede enviar trabajos igualmente a los workers, si conoce la cola en la que está escuchando ('default' por defecto): 
        
        client.check_registry = 'never'
        client.set_default_queue('myqueue')
        
        Las opciones del cliente check_registry ='cache' (por defecto) y check_registry ='always' permiten al cliente utilizar automáticamente las colas
        publicadas para cada método. Hay que tener en cuenta que, en el caso que existan varias colas disponibles para un método (función), 
        sólo se seleccionará una de ellas (automáticamente) para cada tarea (método 'select_queue' de la clase Client). 

        requests_queues_dict es un diccionario con el nombre (corto) de las colas
        de clave y un diccionario con los nombres de las funciones de claves y la función de valor
        """

        registry = {}
        for queue_name, func_dict in requests_queues_dict.items():
            for method in func_dict:
                registry[method] = registry.get(method, []) + [queue_name]

        self.lock_registry(self.ntries_register_lock)
        try:
            for method in registry:
                method_set = f"method_queues_{method}"
                tmp = self.variables.get(method_set, set())
                self.variables[method_set] = tmp.union(registry[method])
                colas = ", ".join(str(q) for q in registry[method])
                logger.info(f"Method {method} published as available for queues: {colas}")
        finally:    
            self.unlock_registry()

    def random_queue_for_method(self, method):
        available = self.all_queues_for_method(method)
        if len(available) == 0:
            return None
        return random.choice(available)

    def all_queues_for_method(self, method):
        method_set = f"method_queues_{method}"
        return [x for x in self.variables[method_set]]

    def enqueue(self, queue_name, msg):
        queue = self.namespace.list(queue_name)
        queue.append(msg)


    def pop(self, queue_name, timeout=0):
        """Blocking pop operation for retrieving first item from a FIFO queue.
    
        Args:
            queue_name: Name of the queue to pop first item from
            timeout: Maximum time to wait in seconds (0 = wait indefinitely)
            
        Returns:
            tuple: (queue_name, value) if first item found, None if timeout occurs

        Note:
            - Used by clients
            - Supports both watchdog and polling modes
        """
        queue = self.namespace.list(queue_name)
        
        wait_forever = (timeout == 0)
        start_time = time.time()
        while wait_forever or (time.time() - start_time) < timeout:
            try:
                return (queue_name, queue.pop(0)) 
            except (IndexError, KeyError):
                pass
            
            if self.with_watchdog:
                fsutils.watchdog.wait_until_file_event(
                    [queue.base_path], 
                    [], 
                    ["created"], 
                    timeout=self.pop_timeout
                )
                sleep(*self.pop_sleep_watchdog)  # Wait random time to minimize probability of race conditions       
            else:
                sleep(*self.pop_sleep)  # Standard polling delay
        
        return None  # Timeout reached
    
    def pop_multiple(self, queue_names, timeout=0):
        """Blocking first item pop from multiple FIFO queues in priority order (highest first).
        
        Args:
            queue_names: List of queue names (ordered by priority - highest first)
            timeout: Maximum wait time in seconds (0 = wait indefinitely)
            
        Returns:
            tuple: (queue_name, value) if item found, None if timeout reached
            
        Note:
            - Used by workers
            - Checks queues in order until item is found
            - Supports both watchdog and polling modes
        """
        queue_refs = [(q, self.namespace.list(q)) for q in queue_names]
        
        if self.with_watchdog:
            watch_paths = [q[1].base_path for q in queue_refs]

        wait_forever = (timeout == 0)
        start = time.time()
        while wait_forever or (time.time() - start) < timeout:
            for q_name, queue in queue_refs:
                try:
                    return (q_name, queue.pop(0))
                except (IndexError, KeyError):
                    continue
            
            if self.with_watchdog:
                fsutils.watchdog.wait_until_file_event(
                    watch_paths,
                    [],
                    ["created"],
                    timeout=self.pop_timeout
                )
                sleep(*self.pop_sleep_watchdog)
            else:
                sleep(*self.pop_sleep)
        
        return None  # Timeout expired

    def pop_all(self, queue_name):
        """Pops (in order) all messages available in queue with name queue_name.
        
        Used by clients.
        """
        queue = self.namespace.list(queue_name)
        N = len(queue)
        return [queue.pop(0) for i in range(N)]
