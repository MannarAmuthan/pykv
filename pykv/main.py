from typing import Dict, Optional

from pykv.handlers.file_handler import FileHandler, FileHandlerImpl


class KeyNotFoundException(Exception):
    pass


class KeyAlreadyExistsException(Exception):
    pass


class KeyValueStore:

    def __init__(self,
                 file_path: Optional[str],
                 file_handler: FileHandler = FileHandlerImpl()):

        self.__file_path__ = file_path
        self.file_handler = file_handler

        self.__store__ = {}

        file_handler.create_file_if_not_exists(file_path=file_path)

    def read(self, key_string: str):
        if key_string not in self.__store__:
            return KeyNotFoundException(f"Given key {key_string} not found in Store")
        return self.__store__[key_string]

    def write(self, key_string: str, key_value: Dict):
        if key_string in self.__store__:
            return KeyAlreadyExistsException(f"Given key {key_string} is already exists in Store")
        self.__store__[key_string] = key_value

    def delete(self, key_string: str):
        if key_string not in self.__store__:
            return KeyNotFoundException(f"Given key {key_string} not found in Store")
        self.__store__.pop(key_string)
