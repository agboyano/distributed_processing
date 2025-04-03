import datetime
import json
import logging
import math
import os
import pickle
import random
import shutil
import time
import uuid
from ast import literal_eval
from dataclasses import dataclass
from decimal import Decimal, localcontext
from pathlib import Path

import joblib

from .watchdog import wait_until_file_event

logger = logging.getLogger(__name__)


def sleep(a, b=None):
    if b is None:
        time.sleep(a)
    else:
        time.sleep(random.uniform(a, b))


@dataclass
class FSSerializer:
    dump: callable
    load: callable
    extension: str


def pickle_dump(value, filename):
    with open(filename, "wb") as f:
        pickle.dump(value, f, protocol=pickle.HIGHEST_PROTOCOL)
        f.flush()
        os.fsync(f.fileno())


def pickle_load(filename):
    with open(filename, "rb") as f:
        return pickle.load(f)


pickle_serializer = FSSerializer(pickle_dump, pickle_load, "pkl")


def json_dump(value, filename):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(value, f)
        f.flush()
        os.fsync(f.fileno())


def json_load(filename):
    with open(filename, "r") as f:
        return json.load(f)


json_serializer = FSSerializer(json_dump, json_load, "json")


def joblib_dump(value, filename):
    with open(filename, "wb") as f:
        joblib.dump(
            value, f
        )  # a joblib.dump le puedo pasar una cadena o un file handler
        f.flush()
        os.fsync(f.fileno())


def joblib_load(filename):
    return joblib.load(filename)


joblib_serializer = FSSerializer(joblib_dump, joblib_load, "jbl")


class FSUDict:
    def __init__(
        self, base_path: str, temp_dir: str, serializer=joblib_serializer, fast=False
    ):
        self.base_path = Path(base_path).resolve()
        self.temp_dir = Path(temp_dir).resolve()

        self.base_path.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)

        self.load = serializer.load
        self.dump = serializer.dump
        self.ext = serializer.extension

        self.fast = fast

    def _key_to_filename(self, key):
        return repr(key) + "." + self.ext

    def _filename_to_key(self, filename):
        return literal_eval(".".join(filename.split(".")[:-1]))

    def __setitem__(self, key, value) :
        """Store value using atomic write pattern"""
        target_path = self.base_path / self._key_to_filename(key)

        if self.fast:
            self.dump(value, target_path)

        else:
            temp_path = self.temp_dir / f"tmp_{uuid.uuid4().hex}"
            try:
                self.dump(value, temp_path)

                try:
                    temp_path.rename(target_path)
                except FileExistsError:
                    target_path.unlink()
                    temp_path.rename(target_path)
            finally:
                if temp_path.exists():
                    temp_path.unlink()

    def __getitem__(self, key):
        target_path = self.base_path / self._key_to_filename(key)
        try:
            return self.load(target_path)
        except FileNotFoundError:
            raise KeyError(key)

    def __delitem__(self, key):
        target_path = self.base_path / self._key_to_filename(key)
        try:
            target_path.unlink()
        except FileNotFoundError:
            raise KeyError(key)

    def __contains__(self, key):
        return (self.base_path / self._key_to_filename(key)).is_file()

    def keys(self):
        return [
            self._filename_to_key(f.name)
            for f in os.scandir(self.base_path)
            if f.is_file()
        ]
        # return [self._filename_to_key(f.name) for f in self.base_path.iterdir() if f.is_file()]

    def values(self):
        return [self[key] for key in self.keys()]

    def items(self):
        return [(key, self[key]) for key in self.keys()]

    def __len__(self):
        return len(self.keys())

    def __iter__(self):
        return (k for k in self.keys())

    def clear(self):
        shutil.rmtree(self.base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)  # por si tmp en base_path
        """
        for key in self.keys():
            del self[key]
        """

    def update(self, iterable):
        for k, v in iterable:
            self[k] = v

    def get(self, key, default=None):
        if key in self:
            return self[key]
        else:
            return default

    def pop(self, key):
        try:
            if self.fast:
                value = self[key]
                del self[key]
            else:
                temp_path = self.temp_dir / f"tmp_{uuid.uuid4().hex}"
                target_path = self.base_path / self._key_to_filename(key)
                target_path.rename(temp_path)
                value = self.load(temp_path)
                temp_path.unlink()
            return value
        except FileNotFoundError:
            raise KeyError(key)


class FSList:
    def __init__(
        self, base_path: str, temp_dir: str, serializer=joblib_serializer, prec=50
    ):
        self.data = FSUDict(base_path, temp_dir, serializer)
        self.base_path = self.data.base_path
        self.serializer = serializer
        self.prec = prec
        self._zero = Decimal(10000)
        self._incr = Decimal(100)
        self.keys_zero = str(self._zero)
        self.keys_incr = str(self._incr)
        self.max_penalty = 1000
        self._zero_date = datetime.datetime.combine(
            datetime.date(2025, 1, 1), datetime.time(0, 0, 0)
        )
        self._zero_timestamp = (
            Decimal(math.ceil(datetime.datetime.timestamp(self._zero_date) * 1000000))
            / 1000000
        )
        self.id = uuid.uuid4().hex  # round(random.random()*1000000)

    def _random_id(self):
        return random.random() * 1000000

    def _seconds_elapsed(self):
        return (
            Decimal(
                math.ceil(
                    datetime.datetime.timestamp(datetime.datetime.now()) * 1000000
                )
            )
            / 1000000
            - self._zero_timestamp
        )

    def _append_key(self):
        return (str(self._seconds_elapsed()), self._random_id())

    def _new_zero_key(self):
        return (str(-self._seconds_elapsed()), self._random_id())

    def _new_mid_key(self, current_key, current_prev_key):
        with localcontext() as ctx:
            ctx.prec = self.prec
            curr = Decimal(current_key[0])
            prev = Decimal(current_prev_key[0])
            new = (curr + prev) / Decimal(2)
            if new == prev:
                raise IndexError(
                    f"Insert: Key collision: Increment precision above {self.prec}"
                )
            return (str(new), self._random_id())

    def append(self, value):
        new_key = self._append_key()
        assert new_key not in self.data
        self.data[new_key] = value

    def extend(self, iterable):
        for x in iterable:
            self.append(x)

    def copy(self):
        return [x for x in self]

    def insert(self, index, value):
        keys = self.keys()
        N = len(keys)

        if index == 0:
            if N == 0:
                self.append(value)
            else:
                self.data[self._new_zero_key()] = value
        elif index >= N:
            self.append(value)
        elif 0 < index < N:
            self.data[self._new_mid_key(keys[index], keys[index - 1])] = value
        else:
            raise IndexError("list assignment index out of range")

    def values(self):
        return [self.data[k] for k in self.keys()]

    def items(self):
        return [(self.data[k], k) for k in self.keys()]

    def keys(self):
        with localcontext() as ctx:
            ctx.prec = self.prec
            return [
                k
                for _, k in sorted(
                    [(Decimal(k[0]), k) for k in self.data.keys()], key=lambda x: x[0]
                )
            ]

    def __delitem__(self, index):
        del self.data[self.data.keys()[index]]

    def clear(self):
        self.data.clear()

    def __getitem__(self, index):
        return self.data[self.keys()[index]]

    def __setitem__(self, index, value):
        self.data[self.keys()[index]] = value

    def __del__(self):
        pass

    def __contains__(self, key):
        return key in [x for x in self]

    def __iter__(self):
        return (self.data[k] for k in self.keys())

    def __len__(self):
        return len(self.keys())

    def pop(self, index=-1):
        ix = self.keys()[index]
        return self.data.pop(ix)

    def pop_left(self):
        keys = self.keys()
        N = len(keys)
        if N == 0:
            raise IndexError("pop_left: empty list")

        i = 0
        while i < N:
            try:
                return self.data.pop(keys[i])
            except KeyError:
                i += random.choice([1, 1, 1, 1, 2, 2, 2, 3, 3, 4])
        raise KeyError("pop_left: something bad happened")


class FSNamespace:
    def __init__(self, base_path: str, temp_dir=None, serializer=joblib_serializer):
        self.base_path = Path(base_path).resolve()
        if temp_dir is None:
            self.temp_dir = self.base_path / "tmp"
        else:
            self.temp_dir = Path(temp_dir).resolve()

        self.base_path.mkdir(parents=True, exist_ok=True)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.serializer = serializer
        self.sep = "_"
        self.udict_prefix = "ud"
        self.list_prefix = "li"
        self.prefixes = {self.udict_prefix, self.list_prefix}

    def udict(self, name):
        nt = [x for x in self.names_types() if x[0] == name]
        if len(nt) > 0 and nt[0][1] != self.udict_prefix:
            raise ValueError(f"{name} exits with type {nt[0][1]}")
        return FSUDict(
            self.base_path / (self.udict_prefix + self.sep + name),
            self.temp_dir,
            self.serializer,
        )

    def list(self, name):
        nt = [x for x in self.names_types() if x[0] == name]
        if len(nt) > 0 and nt[0][1] != self.list_prefix:
            raise ValueError(f"{name} exits with type {nt[0][1]}")
        return FSList(
            self.base_path / (self.list_prefix + self.sep + name),
            self.temp_dir,
            self.serializer,
        )

    def clear(self, clear_tmp=True):
        for d in [
            d for d in self.base_path.iterdir() if d.is_dir() if d != self.temp_dir
        ]:
            shutil.rmtree(d)
        for f in [f for f in self.base_path.iterdir() if f.is_file()]:
            f.unlink()
        self.temp_dir.mkdir(parents=True, exist_ok=True)

    def clear_tmp(self):
        for f in [f for f in self.temp_dir.iterdir() if f.is_file()]:
            f.unlink()

    def _dirname_to_name_type(self, dirname):
        s = (dirname).split(self.sep)
        return (self.sep).join(s[1:]), s[0]

    def names_types(self):
        tmp = [
            self._dirname_to_name_type(d.name)
            for d in self.base_path.iterdir()
            if d.is_dir()
        ]
        return [x for x in tmp if x[1] in self.prefixes]

    def names(self):
        return [x[0] for x in self.names_types()]

    def type(self, name):
        return [x[1] for x in self.names_types() if x[0] == name][0]

    def get(self, name):
        nt = [x for x in self.names_types() if x[0] == name]
        if len(nt) > 0 and nt[0][1] == self.udict_prefix:
            return FSUDict(
                self.base_path / (self.udict_prefix + self.sep + name),
                self.temp_dir,
                self.serializer,
            )

        if len(nt) > 0 and nt[0][1] == self.list_prefix:
            return FSList(
                self.base_path / (self.list_prefix + self.sep + name),
                self.temp_dir,
                self.serializer,
            )
        raise ValueError(f"{name} not available in namespace")


class LockingError(Exception):
    def __init__(self, message):
        super().__init__(message)


def wait_until_lock_released(
    fsudict, lock_name, ntries=5, watchdog_timeout=10, wait=(0.0, 0.0)
):
    for i in range(ntries):
        if lock_name not in fsudict:
            return True

        e, _, _, _ = wait_until_file_event(
            [fsudict.base_path],
            [lock_name],
            ["deleted", "moved"],
            timeout=watchdog_timeout,
        )

        if e is not None:
            return True

        sleep(*wait)

    logger.warning(f"{lock_name} not released: tried {i + 1} times")
    return False


def set_lock(fsudict, lock_name, ntries=5, watchdog_timeout=10, wait=(0.0, 0.0)):
    if lock_name not in fsudict:
        fsudict[lock_name] = True
        logger.debug(f"{lock_name} lock set: tried 0 times")

    elif wait_until_lock_released(fsudict, lock_name, ntries, watchdog_timeout, wait):
        fsudict[lock_name] = True
        logger.debug(f"{lock_name} lock set")
    else:
        logger.warning(f"Error: couldn`t get lock {lock_name}")
        raise LockingError(f"couldn`t get lock {lock_name}")


def unset_lock(fsudict, lock_name):
    del fsudict[lock_name]
    logger.debug("{lock_name} unset")
