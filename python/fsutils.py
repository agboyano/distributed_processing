import os
import re
from datetime import datetime
import joblib
import pickle
import json

def pickle_dump(value, filename):
    with open(filename, 'wb') as f:
        pickle.dump(value, f, protocol=pickle.HIGHEST_PROTOCOL)

def pickle_load(filename):
    with open(filename, 'rb') as f:
        return pickle.load(f)

def json_dump(value, filename):
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(value, f)

def json_load(filename):
    with open(filename, 'r') as f:
        return json.load(f)

def group(lst):
    salida = []
    s = set(lst)
    for x in s:
        salida.append((x, lst.count(x)))
    return salida

def all_lists(directory, serializer=None):
    if serializer == "joblib":
        file_extension = "joblib"
    elif serializer == "json":
        file_extension = "json"
    else:
        file_extension = "pickle"

    def decode_filename(filename):
        pattern = r"namespace_(.*?)_list_(.*?)_order_.*?\." + file_extension
        coincidences = re.search(pattern, filename)

        try:
            return (True, coincidences.group(1), coincidences.group(2))
        except:
            return (False, None, None)
    d = {}
    for filename in os.listdir(directory):
        ok, namespace, name = decode_filename(filename)
        if ok:
            if namespace in d:
                d[namespace] += [name]
            else:
                d[namespace] = [name]
    dd = {}
    for k in d:
        dd[k] = group(d[k])
    return dd

class FSDict():

    def __init__(self, directory, name, namespace="fsdict", reset_order=False, serializer=None):
        self._dir = directory
        os.makedirs(directory, exist_ok=True)
        self._name = name
        self._namespace = namespace 
        self._reset_order = reset_order # resetea order cada vez que inserta una clave ya existente
        if serializer == "joblib":
            self._dump = joblib.dump    
            self._load = joblib.load
            self._file_extension = "joblib"
        elif serializer == "json":
            self._dump = json_dump    
            self._load = json_load
            self._file_extension = "json"
        else:
            self._dump = pickle_dump    
            self._load = pickle_load
            self._file_extension = "pickle"


    def _timestamp(self):
        return datetime.now().strftime('%Y%m%d%H%M%S') + f"{datetime.now().microsecond // 1000:03d}"

    def _encode_key(self, key):
        return f"namespace_{self._namespace}_dict_{self._name}_order_{self._timestamp()}_key_" + repr(key) + f".{self._file_extension}"
    
    def _decode_filename(self, filename):
        pattern = r"namespace_(.*?)_dict_(.*?)_order_(.*?)_key_(.*?)\." + self._file_extension
        coincidences = re.search(pattern, filename)

        # CUIDAO!!!!! Utilizo eval
        try:
            return (True, coincidences.group(1), coincidences.group(2), coincidences.group(3), eval(coincidences.group(4)))
        except:
            return (False, None, None, None, None)

    def _build_first_filename(self, key):
        return os.path.join(self._dir, self._encode_key(key))
    
    def _sorted_keys(self):
        k = []        
        for filename in os.listdir(self._dir):
            ok, ns, name, order, key = self._decode_filename(filename)
            if ok and ns==self._namespace and name==self._name:
                k.append((key, filename, order))
        return [(x[0], x[1]) for x in sorted(k, key=lambda x:x[2], reverse=False)]

    def _find_keys(self):
        return {x[0]:x[1] for x in self._sorted_keys()}

    def load(self, key):
        return self._load(os.path.join(self._dir, self._find_keys()[key]))

    def save(self, key, value):
        ks = self._find_keys()
        if key in ks:
            if self._reset_order:
                del self[key]
                filename = self._build_first_filename(key)
            else:
                filename = os.path.join(self._dir, ks[key])
        else:
            filename = self._build_first_filename(key)

        self._dump(value, filename)

    def remove(self, key): 
        os.remove(os.path.join(self._dir, self._find_keys()[key]))

    def keys(self):
        return self._find_keys().keys()
    
    def values(self):
        return (self[k] for k in self.keys())
    
    def items(self):
        return ((k, self[k]) for k in self.keys())

    def __getitem__(self, key):
        return self.load(key)

    def __setitem__(self, key, value):
        self.save(key, value)
    
    def __delitem__(self, key):
        self.remove(key)

    def clear(self):
        for k in self._find_keys():
            self.remove(k)

    def clear_namespace(self):  
        for filename in os.listdir(self._dir):
            ok, ns, name, order, key = self._decode_filename(filename)
            if ok and ns==self._namespace:
                os.remove(os.path.join(self._dir, filename))

    def __del__(self):
        "Destructor, no hago nada."
        pass

    def __contains__(self, key):
        return key in self.keys()
    
    def __iter__(self):
        return (k for k in self.keys())
    
    def __len__(self):
        return len(self._find_keys())
    
    def get(self, key, default=None):
        if key in self._find_keys():
            return self[key]
        else:
            return default
        
    def pop(self, key):
        value = self[key]
        del self[key]
        return value
    
    # devuelve el último que fue insertado
    def popitem(self):
        try:
            last_k = self._sorted_keys()[-1][0]
        except IndexError:
            raise KeyError('popitem(): dictionary is empty')
        return (last_k, self.pop(last_k))
    
    # devuelve el primero que fue insertado    
    def pop_first_item(self):
        try:
            first_k = self._sorted_keys()[-1][0]
        except IndexError:
            raise KeyError('popitem(): dictionary is empty')
        return (first_k, self.pop(first_k))
    
    def update(self, iterable):
        for k,v in iterable:
            self[k] = v


class FSList():
    
    def __init__(self, directory, name, namespace="fslist", serializer=None):
        self._dir = directory
        os.makedirs(directory, exist_ok=True)
        self._name = name
        self._namespace = namespace 
        self.MINSTEP = 1000000

        if serializer == "joblib":
            self._dump = joblib.dump    
            self._load = joblib.load
            self._file_extension = "joblib"
        elif serializer == "json":
            self._dump = json_dump    
            self._load = json_load
            self._file_extension = "json"
        else:
            self._dump = pickle_dump    
            self._load = pickle_load
            self._file_extension = "pickle"
    
    def _decode_filename(self, filename):
        pattern = r"namespace_(.*?)_list_(.*?)_order_(.*?)\." + self._file_extension
        coincidences = re.search(pattern, filename)

        try:
            return (True, coincidences.group(1), coincidences.group(2), coincidences.group(3))
        except:
            return (False, None, None, None)

    def _sorted_items(self):
        k = []        
        for filename in os.listdir(self._dir):
            ok, ns, name, order= self._decode_filename(filename)
            if ok and ns==self._namespace and name==self._name:
                k.append((filename, order))
        return [(x[0], x[1]) for x in sorted(k, key=lambda x:x[1], reverse=False)]
    
    def _encode_order(self, x):
        return f"{x:020d}"

    def _calculate_order(self, index=None):
        lst = self._sorted_items()

        N = len(lst)

        if index is None: #append
            if lst == []:
                return self._encode_order(self.MINSTEP)
            else:
                return self._encode_order(int(lst[-1][1]) + self.MINSTEP)

        if index < 0:
            i = N + index
        else:
            i = index
        
        if i < 0 or i > (N-1):
            raise IndexError("list index out of range")
        
        if i == 0:
            prev_order = 0
        else:
            prev_order = int(lst[i-1][1])

        return self._encode_order((int(lst[i][1]) + prev_order) //2)

    def _encode_filename(self, index=None):
        order = self._calculate_order(index)
        return f"namespace_{self._namespace}_list_{self._name}_order_{order}.{self._file_extension}"

    def _build_filename(self, index=None):
        return os.path.join(self._dir, self._encode_filename(index))

    def load(self, filename):
        return self._load(os.path.join(self._dir, filename))
    
    def append(self, value):
        self._dump(value, self._build_filename())

    def extend(self, iterable):
        for x in iterable:
            self.append(x)
    
    def copy(self):
        return [x for x in self]

    def insert(self, index, value):
        self._dump(value, self._build_filename(index))

    def __delitem__(self, key):
        lst = self._sorted_items()
        os.remove(os.path.join(self._dir, lst[key][0]))

    def clear(self):
        for k, _ in self._sorted_items():
            os.remove(os.path.join(self._dir, k))

    def __getitem__(self, key):
        lst = self._sorted_items()
        return self._load(os.path.join(self._dir, lst[key][0]))

    def __setitem__(self, key, value):
        lst = self._sorted_items()
        self._dump(value, lst[key][0])

    def clear_namespace(self):  
        for filename in os.listdir(self._dir):
            ok, ns, name, order = self._decode_filename(filename)
            if ok and ns==self._namespace:
                os.remove(os.path.join(self._dir, filename))

    def __del__(self):
        "Destructor, no hago nada."
        pass

    def __contains__(self, key):
        return key in [x for x in self]
    
    def __iter__(self):
        lst = self._sorted_items()
        return (self._load(os.path.join(self._dir, f)) for f,_ in self._sorted_items())
    
    def __len__(self):
        return len(self._sorted_items())
    
    def pop(self, key=-1):
        value = self[key]
        del self[key]
        return value
