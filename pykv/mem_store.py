from datetime import datetime
from time import sleep
from typing import Dict

from pykv.data_structures.heap import Heap
from pykv.store import StoreException, Store
from pykv.utils import get_timestamp, add_seconds, is_passed


class MemStore(Store):
    def __init__(self, background_jobs_frequency_in_seconds):
        self.keys_and_values = {}
        self.keys_and_ttls = {}
        self.heap = Heap()
        self.background_jobs_frequency_in_seconds = background_jobs_frequency_in_seconds

        super().__init__(self.expire_keys,
                         background_jobs_frequency_in_seconds)

    def is_exists(self, key_string):
        return key_string in self.keys_and_values

    def create(self, key_string: str, key_value: Dict, time_to_live_in_seconds: int = 0):
        self.keys_and_values[key_string] = key_value
        time_to_live = get_timestamp(add_seconds(datetime.now())) if time_to_live_in_seconds > 0 else 0
        if time_to_live > 0:
            self.keys_and_ttls[key_string] = time_to_live
            self.heap.push((time_to_live, key_string))

    def get(self, key_string: str):
        if self.is_exists(key_string):
            if key_string in self.keys_and_ttls and is_passed(self.keys_and_ttls[key_string]):
                self.delete(key_string)
                raise StoreException(f"Attempt to retrieve expired key {key_string}")

            return self.keys_and_values[key_string]

    def delete(self, key_string: str):
        if self.is_exists(key_string):
            self.keys_and_values.pop(key_string)
        if key_string in self.keys_and_ttls:
            self.keys_and_ttls.pop(key_string)

    def get_all_keys_and_values(self):
        return self.keys_and_values

    def expire_keys(self):
        while not self.stop_event.is_set():
            while not self.heap.is_empty():
                ttl_in_seconds, key_string = self.heap.peek()
                if is_passed(ttl_in_seconds):
                    self.delete(key_string)
                    self.heap.pop()
                else:
                    break
            for _ in range(0, self.background_jobs_frequency_in_seconds):
                if self.stop_event.is_set():
                    break
                sleep(1)
