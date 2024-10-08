import json
import mmap
import os
from typing import Dict

from pykv.record import RecordManager


def create_file_if_not_exists(file_path: str):
    if not os.path.exists(file_path):
        file = open(file_path, "w")
        file.close()
        return True
    return False


def extend_file(bytes_to_append: int, file_path: str):
    if os.path.exists(file_path):
        f = open(file_path, "ab")
        f.write(bytes_to_append * b'\0')
        f.close()


def get_memory_mapped_file_pointer(file_path):
    with open(file_path, "r+b") as f:
        return mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_WRITE)


class FileStore:
    def __init__(self, file_path: str):

        self.keys_and_offsets: Dict[str, int] = {}
        self.current_slot = 0
        self.starting_offset = 0
        self.total_blocks = 10
        self.block_size_in_bytes = 512
        self.file_path = file_path

        self.record_manager = RecordManager()

        if create_file_if_not_exists(file_path):
            extend_file(bytes_to_append=self.total_blocks * self.block_size_in_bytes,
                        file_path=file_path)

        self.file_pointer = get_memory_mapped_file_pointer(file_path)

    def is_exists(self, key_string: str):
        if key_string in self.keys_and_offsets:
            return True
        return False

    def get(self, key_string: str):
        record_offset = self.starting_offset + (self.keys_and_offsets[key_string] * self.block_size_in_bytes)
        if self.is_exists(key_string) and self.record_manager.is_available(
                file_pointer=self.file_pointer,
                record_offset=record_offset):
            _, _, _, _, _, value_as_bytes = self.record_manager.read(
                file_pointer=self.file_pointer,
                record_offset=record_offset)

            return json.loads(value_as_bytes)

    def create(self, key_string: str, key_value: Dict):
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

            self.record_manager.write(
                file_pointer=self.file_pointer,
                offset=self.starting_offset + (self.current_slot * self.block_size_in_bytes),
                key_as_bytes=key_as_bytes,
                value_as_bytes=value_as_bytes
            )

            self.keys_and_offsets[key_string] = self.current_slot
            self.current_slot += number_of_slots_needed

    def delete(self, key_string: str):
        if self.is_exists(key_string):
            self.record_manager.delete(
                self.file_pointer,
                self.starting_offset + (self.keys_and_offsets[key_string] * self.block_size_in_bytes))

            self.keys_and_offsets.pop(key_string)

    def get_all_keys_and_values(self) -> Dict[str, dict]:

        all_keys_and_values = {}

        block = 0
        while block < self.current_slot:
            record_offset = self.starting_offset + (block * self.block_size_in_bytes)
            if self.record_manager.is_available(file_pointer=self.file_pointer,
                                                record_offset=record_offset):
                _, slots_count, _, _, key_as_bytes, value_as_bytes = self.record_manager.read(
                    self.file_pointer, record_offset
                )

                all_keys_and_values[key_as_bytes.decode("utf-8")] = json.loads(value_as_bytes.decode('utf=8'))

                block += slots_count
            else:
                block += 1

        return all_keys_and_values
