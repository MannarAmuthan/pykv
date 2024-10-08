import os
from typing import Dict, Optional

from pykv.file_store import FileStore


class KeyNotFoundException(Exception):
    pass


class KeyAlreadyExistsException(Exception):
    pass


class KeyValueStore:

    def __init__(self,
                 file_path: Optional[str] = "default_kv.bin"
                 ):

        self.__file_path__ = file_path
        self.file_store: FileStore = FileStore(os.getcwd() + "/" + file_path)

    def read(self, key_string: str):
        if not self.file_store.is_exists(key_string):
            return KeyNotFoundException(f"Given key {key_string} not found in Store")
        return self.file_store.get(key_string)

    def write(self, key_string: str, key_value: Dict):
        if self.file_store.is_exists(key_string):
            return KeyAlreadyExistsException(f"Given key {key_string} is already exists in Store")

        self.file_store.create(key_string, key_value)

    def get_all(self):
        return self.file_store.get_all_keys_and_values()
