import json
import os
from sys import getsizeof
from typing import Dict, Optional

from pykv.file_store import FileStore, StoreException
from pykv.mem_store import MemStore


class KeyNotFoundException(Exception):
    pass


class KeyAlreadyExistsException(Exception):
    pass


class ExpiredKeyException(Exception):
    pass


class InvalidKeyException(Exception):
    pass


class InvalidValueException(Exception):
    pass


class KeyValueStore:

    def __init__(self,
                 file_path: Optional[str] = "default_kv.bin",
                 background_jobs_frequency_in_seconds=60,
                 mem_store_mode=False):

        self.__file_path__ = file_path
        self.file_store = FileStore(
            file_path=os.getcwd() + "/" + file_path,
            background_jobs_frequency_in_seconds=background_jobs_frequency_in_seconds)
        self.mem_store: MemStore = MemStore(
            background_jobs_frequency_in_seconds=background_jobs_frequency_in_seconds)

        if mem_store_mode:
            self.store = self.mem_store
        else:
            self.store = self.file_store

        self.store.start_background_jobs()

    def read(self, key_string: str):
        if not self.store.is_exists(key_string):
            raise KeyNotFoundException(f"Given key {key_string} not found in Store")
        try:
            return self.store.get(key_string)
        except StoreException as exception:
            raise ExpiredKeyException(str(exception))

    def delete(self, key_string: str):
        if not self.store.is_exists(key_string):
            raise KeyNotFoundException(f"Given key {key_string} not found in Store")
        return self.store.delete(key_string)

    def write(self, key_string: str, value: Dict, time_to_live_in_seconds=0):

        if len(key_string) > 32:
            raise InvalidKeyException(f"Key string length exceeds limit of 32 characters")

        if getsizeof(json.dumps(value)) > 16000:
            raise InvalidKeyException(f"Value size exceeds limit of 16KB")

        if self.store.is_exists(key_string):
            raise InvalidValueException(f"Given key {key_string} is already exists in Store")

        self.store.create(key_string, value, time_to_live_in_seconds)

    def get_all(self):
        return self.store.get_all_keys_and_values()

    def stop_background_jobs(self):
        self.store.stop_background_jobs()
