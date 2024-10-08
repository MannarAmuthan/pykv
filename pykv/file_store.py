import json
import math
import mmap
import os
from typing import Dict


def create_file_if_not_exists(file_path: str):
    if not os.path.exists(file_path):
        file = open(file_path, "w")
        file.close()
        return True
    return False


def extend_file(bytes_to_append: int, file_path: str):
    if os.path.exists(file_path):
        f = open(file_path, "wb")
        f.write(bytes_to_append * b'\0')
        f.close()


class FileStore:
    def __init__(self, file_path: str):

        self.keys_and_offsets: Dict[str, int] = {}
        self.current_block = 0
        self.starting_offset = 0
        self.total_blocks = 100
        self.block_size_in_bytes = 512
        self.file_path = file_path

        if create_file_if_not_exists(file_path):
            extend_file(bytes_to_append=self.total_blocks * self.block_size_in_bytes,
                        file_path=file_path)

        with open(file_path, "r+b") as f:
            self.file_pointer = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_WRITE)

    def is_exists(self, key_string: str):
        if key_string in self.keys_and_offsets:
            return True
        return False

    def get(self, key_string: str):
        if self.is_exists(key_string):
            record_tuple = self.read_record(self.keys_and_offsets[key_string])
            return json.loads(record_tuple[1])

    def create(self, key_string: str, key_value: Dict):
        if not self.is_exists(key_string):
            key_as_bytes = str.encode(key_string)
            value_as_bytes = str.encode(json.dumps(key_value))
            number_of_used_records: int = self.write_record(
                offset=self.starting_offset + (self.current_block * self.block_size_in_bytes),
                key_as_bytes=key_as_bytes,
                value_as_bytes=value_as_bytes
            )

            self.keys_and_offsets[key_string] = self.current_block
            self.current_block += number_of_used_records

    def write_record(self, offset: int, key_as_bytes: bytes, value_as_bytes: bytes):

        key_len: int = len(key_as_bytes)
        value_len: int = len(value_as_bytes)

        usable_bytes_in_a_record = self.block_size_in_bytes - 5
        needed_records_count = math.ceil((key_len + value_len) * 1.0 / usable_bytes_in_a_record)

        if needed_records_count >= self.total_blocks:
            self.file_pointer.close()
            extend_file(bytes_to_append=needed_records_count * 2 * self.block_size_in_bytes,
                        file_path=self.file_path)
            with open(self.file_path, "r+b") as f:
                self.file_pointer = mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_WRITE)

            self.total_blocks += needed_records_count * 2

        self.file_pointer.seek(offset)
        self.file_pointer.write(b'1')

        offset += 1

        self.file_pointer.seek(offset)
        self.file_pointer.write(needed_records_count.to_bytes(1))

        offset += 1

        self.file_pointer.seek(offset)
        self.file_pointer.write(key_len.to_bytes(1))

        offset += 1

        self.file_pointer.seek(offset)
        self.file_pointer.write(value_len.to_bytes(2, byteorder='big'))

        offset += 2
        self.file_pointer.seek(offset)
        self.file_pointer.write(key_as_bytes)

        offset += key_len

        byte_array = list(value_as_bytes)

        available_bytes_in_current_record = usable_bytes_in_a_record - key_len
        remaining_bytes_to_write = value_len

        starts_at = 0
        while True:

            bytes_to_write = bytes(byte_array[starts_at:starts_at + available_bytes_in_current_record])
            self.file_pointer.seek(offset)
            self.file_pointer.write(bytes_to_write)
            remaining_bytes_to_write = remaining_bytes_to_write - available_bytes_in_current_record

            starts_at = starts_at + available_bytes_in_current_record

            if remaining_bytes_to_write <= 0:
                break

            offset += available_bytes_in_current_record
            offset += 1

            self.file_pointer.seek(offset)
            self.file_pointer.write(b'2')

            available_bytes_in_current_record = usable_bytes_in_a_record

            offset += 4

        return needed_records_count

    def read_record(self, block_number: int):
        block_offset = block_number * self.block_size_in_bytes

        sub_blocks_length_pointer = block_offset + 1
        key_len_pointer = block_offset + 2
        value_len_pointer = block_offset + 3
        key_value_bytes_starts_at = 5

        self.file_pointer.seek(sub_blocks_length_pointer)
        sub_blocks_length = int.from_bytes(self.file_pointer.read(1))

        self.file_pointer.seek(key_len_pointer)
        key_len_in_bytes = self.file_pointer.read(1)

        self.file_pointer.seek(value_len_pointer)
        val_len_in_bytes = self.file_pointer.read(2)

        key_len = int.from_bytes(key_len_in_bytes, 'big')
        val_len = int.from_bytes(val_len_in_bytes, 'big')

        key_pointer: int = block_offset + key_value_bytes_starts_at
        value_pointer: int = block_offset + key_value_bytes_starts_at + key_len

        self.file_pointer.seek(key_pointer)
        key_as_bytes = self.file_pointer.read(key_len)

        # self.file_pointer.seek(value_pointer)
        # value_as_bytes = self.file_pointer.read(val_len)

        value_as_byte_list = []

        usable_bytes_in_a_record = self.block_size_in_bytes - 5
        available_bytes_in_current_record = usable_bytes_in_a_record - key_len
        offset = value_pointer
        bytes_to_read = min(val_len, available_bytes_in_current_record)
        remaining_to_read = val_len

        while True:

            self.file_pointer.seek(offset)
            bytes_read = self.file_pointer.read(bytes_to_read)
            value_as_byte_list.extend(list(bytes_read))
            remaining_to_read = remaining_to_read - bytes_to_read

            if remaining_to_read <= 0:
                break

            offset += available_bytes_in_current_record
            offset += 5
            available_bytes_in_current_record = usable_bytes_in_a_record

            bytes_to_read = min(remaining_to_read,
                                available_bytes_in_current_record)

        return key_as_bytes, bytes(value_as_byte_list), sub_blocks_length

    def get_all_keys_and_values(self) -> Dict[str, dict]:

        all_keys_and_values = {}

        block = 0
        while True:
            if block >= self.current_block:
                break
            key_as_bytes, value_as_bytes, sub_blocks_length = self.read_record(block)

            all_keys_and_values[key_as_bytes.decode("utf-8")] = json.loads(value_as_bytes.decode('utf=8'))

            block += sub_blocks_length

        return all_keys_and_values
