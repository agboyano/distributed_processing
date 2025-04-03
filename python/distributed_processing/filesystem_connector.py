import logging
import random
import time

import fsutils


def sleep(a, b=None):
    if b is None:
        time.sleep(a)
    else:
        time.sleep(random.uniform(a, b))


logger = logging.getLogger(__name__)


class FileSystemConnector(object):
    def __init__(
        self, base_path, temp_dir=None, serializer=fsutils.structs.joblib_serializer
    ):
        self.namespace = fsutils.structs.FSNamespace(base_path, temp_dir, serializer)
        self.variables = self.namespace.udict("variables")
        self.with_watchdog = True
        self.ntries_register_lock = 5
        self.pop_sleep = (5, 10)  # sólo si with_watchdog es False
        self.pop_sleep_watchdog = (0.0, 0.5)
        self.pop_timeout = 60
        self.registry_timeout = 10

    def clean_namespace(self):
        "Borra todos los objetos vinculados al namespace"
        self.namespace.clear()
        self.variables = self.namespace.udict("variables")

    def get_requests_queue(self, queue_name):
        return f"requests_{queue_name}"

    def get_responses_queue(self, client_id):
        return f"{client_id}_responses"

    def get_reply_to_from_id(self, id_str):
        return id_str.split(":")[0] + "_responses"

    def wait_until_lock_release(
        self, lock_name, ntries=5, watchdog_timeout=10, wait=(0.0, 0.1)
    ):
        for i in range(ntries):
            if lock_name not in self.variables:
                return True
            e, _, _, _ = fsutils.watchdog.wait_until_file_event(
                [self.variables.base_path],
                [lock_name],
                ["deleted", "moved"],
                timeout=watchdog_timeout,
            )
            if e is not None:
                return True
            sleep(*wait)

        logger.warning(f"{lock_name} not released: tried {i + 1} times")
        return False

    def set_lock(self, lock_name, ntries=5, timeout=10):
        if lock_name not in self.variables:
            ok = True
            logger.debug(f"{lock_name} lock set: tried 0 times")
        elif self.wait_until_lock_release(lock_name, ntries, timeout):
            ok = True
            logger.debug(f"{lock_name} lock set")
        else:
            ok = False
            logger.warning(f"{lock_name} set anyway")

        self.variables[lock_name] = ok
        return ok

    def unset_lock(self, lock_name):
        del self.variables[lock_name]
        logger.debug("{lock_name} unset")

    def get_client_id(self):
        self.set_lock("nclients_lock", self.ntries_register_lock)
        nclients = self.variables.get("nclients", 0) + 1
        self.variables["nclients"] = nclients
        self.unset_lock("nclients_lock")
        return f"fs_client_{nclients}"

    def get_server_id(self):
        self.set_lock("nservers_lock", self.ntries_register_lock)
        nservers = self.variables.get("nservers", 0) + 1
        self.variables["nservers"] = nservers
        self.unset_lock("nservers_lock")
        return f"fs_server_{nservers}"

    def methods_registry(self):
        """
        Lo usa el cliente.
        Cada método tiene un set de redis con clave {namespace}:method_queues:{method}.
        El contenido del set son los nombres de las colas donde se pueden enviar
        los request para ejecutar ese método.
        """
        registry = {}
        self.set_lock("registry_lock", self.ntries_register_lock)
        try:
            method_queues = [x for x in self.variables.keys() if "method_queues_" in x]
        finally:
            self.unset_lock("registry_lock")

        for method_set in method_queues:
            method = method_set.replace("method_queues_", "")
            available = [x for x in self.variables[method_set]]
            registry[method] = available

        return registry

    def register_methods(self, requests_queues_dict):
        """Registers worker's public functions and their associated FIFO queues.

        Args:
            requests_queues_dict: A dictionary mapping queue names to method
                                  dictionaries, where each method dictionary has
                                  function names as keys and callable functions as values.

                                  {'queue_name': {'function_name': callable_function, ...}, ... }

        Client Configuration Options:
            - check_registry='never': Clients must manually specify queues with set_default_queue().
            - check_registry='cache' (default): Clients can update the cache with update_registry().
            - check_registry='always': Client always checks the latest registry before dispatching.
                                       Huge overhead.

        Note:
            - The Client method select_queue() selects the queue to use.
        """

        registry = {}
        for queue_name, func_dict in requests_queues_dict.items():
            for method in func_dict:
                registry[method] = registry.get(method, []) + [queue_name]

        self.set_lock("registry_lock", self.ntries_register_lock)
        try:
            for method in registry:
                method_set = f"method_queues_{method}"
                tmp = self.variables.get(method_set, set())
                self.variables[method_set] = tmp.union(registry[method])
                colas = ", ".join(str(q) for q in registry[method])
                logger.info(
                    f"Method {method} published as available for queues: {colas}"
                )
        finally:
            self.unset_lock("registry_lock")

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

        wait_forever = timeout == 0
        start_time = time.time()
        while wait_forever or (time.time() - start_time) < timeout:
            try:
                return (queue_name, queue.pop(0))
            except (IndexError, KeyError):
                pass

            if self.with_watchdog:
                _ = fsutils.watchdog.wait_until_file_event(
                    [queue.base_path], [], ["created"], timeout=self.pop_timeout
                )
                # Wait random time to minimize probability of race conditions
                sleep(*self.pop_sleep_watchdog)
            else:
                sleep(*self.pop_sleep)  # Standard polling delay

        return None  # Timeout reached

    def pop_multiple(self, queue_names, timeout=0):
        """Blocking pop(0) from multiple FIFO queues in priority order (highest first).

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

        wait_forever = timeout == 0
        start = time.time()
        while wait_forever or (time.time() - start) < timeout:
            for q_name, queue in queue_refs:
                try:
                    return (q_name, queue.pop(0))
                except (IndexError, KeyError):
                    continue

            if self.with_watchdog:
                _ = fsutils.watchdog.wait_until_file_event(
                    watch_paths, [], ["created"], timeout=self.pop_timeout
                )
                sleep(*self.pop_sleep_watchdog)
            else:
                sleep(*self.pop_sleep)

        return None  # Timeout expired

    def pop_all(self, queue_name):
        """Pops all available messages in the queue named queue_name (in order).

        Used by clients.
        """
        queue = self.namespace.list(queue_name)
        N = len(queue)
        return [queue.pop(0) for i in range(N)]
