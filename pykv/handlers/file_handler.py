import abc
import os


class FileHandler:

    @abc.abstractmethod
    def create_file_if_not_exists(self, file_path: str):
        pass


class FileHandlerImpl(FileHandler):

    def __init__(self):
        pass

    def create_file_if_not_exists(self, file_path: str):
        if not os.path.exists(os.getcwd() + "/" + file_path):
            open(file_path, "w").close()
