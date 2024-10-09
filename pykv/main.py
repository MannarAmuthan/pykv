import json
import os
from sys import getsizeof
from typing import Dict, Optional

from pykv.file_store import FileStore, FileStoreGetException


class KeyNotFoundException(Exception):
    pass


class KeyAlreadyExistsException(Exception):
    pass


class InvalidKeyException(Exception):
    pass


class ExpiredKeyException(Exception):
    pass


class InvalidValueException(Exception):
    pass


class KeyValueStore:

    def __init__(self,
                 file_path: Optional[str] = "default_kv.bin"
                 ):

        self.__file_path__ = file_path
        self.file_store: FileStore = FileStore(os.getcwd() + "/" + file_path)

    def read(self, key_string: str):
        if not self.file_store.is_exists(key_string):
            raise KeyNotFoundException(f"Given key {key_string} not found in Store")
        try:
            return self.file_store.get(key_string)
        except FileStoreGetException as exception:
            raise ExpiredKeyException(str(exception))

    def delete(self, key_string: str):
        if not self.file_store.is_exists(key_string):
            raise KeyNotFoundException(f"Given key {key_string} not found in Store")
        return self.file_store.delete(key_string)

    def write(self, key_string: str, value: Dict, time_to_live_in_seconds=0):

        if len(key_string) > 32:
            raise InvalidKeyException(f"Key string length exceeds limit of 32 characters")

        if getsizeof(json.dumps(value)) > 16000:
            raise InvalidKeyException(f"Value size exceeds limit of 16KB")

        if self.file_store.is_exists(key_string):
            raise InvalidValueException(f"Given key {key_string} is already exists in Store")

        self.file_store.create(key_string, value, time_to_live_in_seconds)

    def get_all(self):
        return self.file_store.get_all_keys_and_values()
