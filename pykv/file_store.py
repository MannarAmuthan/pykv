import json
import os
from datetime import datetime
from time import sleep
from typing import Dict

from pykv.data_structures.heap import Heap
from pykv.record import RecordManager
from pykv.store import StoreGetException, StoreException, Store
from pykv.utils import create_file_if_not_exists, extend_file, get_memory_mapped_file_pointer, is_passed, add_seconds, \
    get_timestamp


class FileStore(Store):
    def __init__(self,
                 file_path: str,
                 background_jobs_frequency_in_seconds: int):

        self.keys_and_offsets: Dict[str, int] = {}
        self.current_slot = 0
        self.starting_offset = 10
        self.total_blocks = 10
        self.record_count = 0
        self.block_size_in_bytes = 512
        self.file_path = file_path
        self.record_manager = RecordManager()
        self.heap = Heap()

        if create_file_if_not_exists(file_path):
            extend_file(bytes_to_append=self.total_blocks * self.block_size_in_bytes,
                        file_path=file_path)
            self.file_pointer = get_memory_mapped_file_pointer(file_path)
            self.record_manager.write_magic_bytes(self.file_pointer)
        else:
            self.file_pointer = get_memory_mapped_file_pointer(file_path)
            self.load()

        super().__init__(self.expire_keys,
                         background_jobs_frequency_in_seconds)

    def is_exists(self, key_string: str):
        if key_string in self.keys_and_offsets:
            return True
        return False

    def get(self, key_string: str):
        record_offset = self.starting_offset + (self.keys_and_offsets[key_string] * self.block_size_in_bytes)
        if self.is_exists(key_string) and self.record_manager.is_available(
                file_pointer=self.file_pointer,
                record_offset=record_offset):
            _, _, ttl_in_seconds, _, _, _, value_as_bytes = self.record_manager.read(
                file_pointer=self.file_pointer,
                record_offset=record_offset)

            if ttl_in_seconds > 0 and is_passed(ttl_in_seconds):
                self.delete(key_string)
                raise StoreGetException(f"Attempt to retrieve expired key {key_string}")

            return json.loads(value_as_bytes)

    def create(self, key_string: str, key_value: Dict, time_to_live_in_seconds: int = 0):
        if not self.is_exists(key_string):
            key_as_bytes = str.encode(key_string)
            value_as_bytes = str.encode(json.dumps(key_value))

            number_of_slots_needed = self.record_manager.get_slots_needed(len(key_as_bytes), len(value_as_bytes))

            if self.total_blocks <= self.current_slot + number_of_slots_needed:
                existing_slots = self.total_blocks - self.current_slot
                slot_shortage = number_of_slots_needed - existing_slots
                self.file_pointer.flush()
                extend_file(bytes_to_append=(2 * slot_shortage) * self.block_size_in_bytes,
                            file_path=self.file_path)
                self.file_pointer = get_memory_mapped_file_pointer(self.file_path)

            time_to_live = get_timestamp(add_seconds(datetime.now())) if time_to_live_in_seconds > 0 else 0
            self.record_manager.write(
                file_pointer=self.file_pointer,
                offset=self.starting_offset + (self.current_slot * self.block_size_in_bytes),
                key_as_bytes=key_as_bytes,
                value_as_bytes=value_as_bytes,
                ttl_in_seconds=time_to_live
            )

            if time_to_live_in_seconds > 0:
                self.heap.push((time_to_live, key_string))

            self.keys_and_offsets[key_string] = self.current_slot
            self.current_slot += number_of_slots_needed
            self.record_count += 1
            self.record_manager.set_record_count(self.file_pointer, self.record_count)

    def delete(self, key_string: str):
        if self.is_exists(key_string):
            self.record_manager.delete(
                self.file_pointer,
                self.starting_offset + (self.keys_and_offsets[key_string] * self.block_size_in_bytes))

            self.keys_and_offsets.pop(key_string)
            self.record_count -= 1
            self.record_manager.set_record_count(self.file_pointer, self.record_count)

    def load(self):

        if not self.record_manager.is_magic_bytes_exists(self.file_pointer):
            raise StoreException("Existing data file is not valid, failed to load")

        slot = 0
        added_records = 0

        self.record_count = self.record_manager.get_record_count(self.file_pointer)

        while added_records < self.record_count:
            record_offset = self.starting_offset + (slot * self.block_size_in_bytes)
            if self.record_manager.is_available(file_pointer=self.file_pointer,
                                                record_offset=record_offset):
                _, slots_count, _, _, _, key_as_bytes, value_as_bytes = self.record_manager.read(
                    self.file_pointer, record_offset
                )

                self.keys_and_offsets[key_as_bytes.decode("utf-8")] = record_offset

                slot += slots_count
                added_records += 1
            else:
                slot += 1

    def get_all_keys_and_values(self) -> Dict[str, dict]:

        all_keys_and_values = {}

        slot = 0
        added_records = 0

        while added_records < self.record_count:
            record_offset = self.starting_offset + (slot * self.block_size_in_bytes)
            if self.record_manager.is_available(file_pointer=self.file_pointer,
                                                record_offset=record_offset):
                _, slots_count, _, _, _, key_as_bytes, value_as_bytes = self.record_manager.read(
                    self.file_pointer, record_offset
                )

                all_keys_and_values[key_as_bytes.decode("utf-8")] = json.loads(value_as_bytes.decode('utf=8'))

                slot += slots_count
                added_records += 1
            else:
                slot += 1

        return all_keys_and_values

    def expire_keys(self):
        while not self.stop_event.is_set():
            while not self.heap.is_empty():
                ttl_in_seconds, key_string = self.heap.peek()
                if is_passed(ttl_in_seconds):
                    self.delete(key_string)
                    self.heap.pop()
                else:
                    break
            sleep(self.background_jobs_frequency_in_seconds)

    def close(self):
        if os.path.exists(self.file_path):
            os.remove(self.file_path)
