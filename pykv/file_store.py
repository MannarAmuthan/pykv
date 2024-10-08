import json
import mmap
import os
from typing import Dict


def create_file_if_not_exists(file_path: str):
    if not os.path.exists(file_path):
        file = open(file_path, "w")
        file.close()
        return True
    return False


class FileStore:
    def __init__(self, file_path: str):

        self.keys_and_offsets: Dict[str, int] = {}
        self.current_block = 0
        self.starting_offset = 0
        self.block_size = 10

        if create_file_if_not_exists(file_path):
            f = open(file_path, "wb")
            f.write(self.block_size * 512 * b'\0')
            f.close()

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
            self.write_record(self.starting_offset + (self.current_block * 512),
                              key_as_bytes,
                              value_as_bytes)

            self.keys_and_offsets[key_string] = self.current_block
            self.current_block += 1
            if self.current_block >= self.block_size:
                self.block_size += 10

    def write_record(self, offset: int, key_as_bytes: bytes, value_as_bytes: bytes):

        key_len: int = len(key_as_bytes)
        value_len: int = len(value_as_bytes)

        self.file_pointer.seek(offset)
        self.file_pointer.write(b'1')

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
        self.file_pointer.seek(offset)
        self.file_pointer.write(value_as_bytes)

    def read_record(self, block_number: int):
        block_offset = block_number * 512

        key_len_pointer = block_offset + 1
        value_len_pointer = block_offset + 2
        self.file_pointer.seek(key_len_pointer)
        key_len_in_bytes = self.file_pointer.read(1)
        self.file_pointer.seek(value_len_pointer)
        val_len_in_bytes = self.file_pointer.read(2)
        key_len = int.from_bytes(key_len_in_bytes, 'big')
        val_len = int.from_bytes(val_len_in_bytes, 'big')
        key_pointer = block_offset + 4
        value_pointer = block_offset + 4 + key_len
        self.file_pointer.seek(key_pointer)
        key_as_bytes = self.file_pointer.read(key_len)
        self.file_pointer.seek(value_pointer)
        value_as_bytes = self.file_pointer.read(val_len)
        return key_as_bytes, value_as_bytes

    def get_all_keys_and_values(self) -> Dict[str, dict]:

        all_keys_and_values = {}

        for block in range(0, self.current_block):
            key_as_bytes, value_as_bytes = self.read_record(block)

            all_keys_and_values[key_as_bytes.decode("utf-8")] = json.loads(value_as_bytes.decode('utf=8'))

        return all_keys_and_values
