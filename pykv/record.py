import functools
import math
import os
import threading
from mmap import mmap
from typing import Tuple, Any, Optional

TYPE_FLAG = int  # 1 byte , 0 - deleted/Empty/available to store , 1 - Primary, 2 - Sub slot
SLOTS_COUNT = int  # 1 byte , length of slots for this record, applicable only for primary slot
TIME_TO_LIVE = int  # 4 byte, UNIX Epoch timestamp
KEY_LEN = int  # 1 byte , length of key byte string
VAL_LEN = int  # 2 byte , length of value byte string
KEY_BYTES = bytes
VALUE_BYTES = bytes

Record = Tuple[TYPE_FLAG, SLOTS_COUNT, TIME_TO_LIVE, KEY_LEN, VAL_LEN, KEY_BYTES, VALUE_BYTES]


def extend_file(bytes_to_append: int, file_path: str):
    if os.path.exists(file_path):
        f = open(file_path, "wb")
        f.write(bytes_to_append * b'\0')
        f.close()


def thread_safe(func):
    def wrapper(self, *args, **kwargs):
        with getattr(self, "file_lock"):
            return func(self, *args, **kwargs)
    return wrapper


class RecordManager:

    def __init__(self):
        self.slot_size_in_bytes = 512
        self.magic_number = 99
        self.metadata_bytes_length_in_a_slot = 9
        self.usable_bytes_in_a_slot = self.slot_size_in_bytes - self.metadata_bytes_length_in_a_slot
        self.file_lock = threading.Lock()

    @thread_safe
    def write_magic_bytes(self, file_pointer):
        file_pointer.seek(0)
        file_pointer.write(self.magic_number.to_bytes(1))

    @thread_safe
    def is_magic_bytes_exists(self, file_pointer):
        file_pointer.seek(0)
        value = int.from_bytes(file_pointer.read(1))
        return self.magic_number == value

    @staticmethod
    def set_record_count(file_pointer, value):
        file_pointer.seek(1)
        file_pointer.write(value.to_bytes(4, 'big'))

    @staticmethod
    def get_record_count(file_pointer):
        file_pointer.seek(1)
        record_count = int.from_bytes(file_pointer.read(4), 'big')
        return record_count

    @thread_safe
    def write(self,
              file_pointer,
              offset: int,
              key_as_bytes: bytes,
              value_as_bytes: bytes,
              ttl_in_seconds: int = 0) -> SLOTS_COUNT:

        key_len: int = len(key_as_bytes)
        value_len: int = len(value_as_bytes)

        usable_bytes_in_a_record = self.slot_size_in_bytes - 5

        slots_count = self.get_slots_needed(key_len, value_len)

        file_pointer.seek(offset)
        file_pointer.write((1).to_bytes(1))

        offset += 1

        file_pointer.seek(offset)
        file_pointer.write(slots_count.to_bytes(1))

        offset += 1

        file_pointer.seek(offset)
        file_pointer.write(ttl_in_seconds.to_bytes(4))

        offset += 4

        file_pointer.seek(offset)
        file_pointer.write(key_len.to_bytes(1))

        offset += 1

        file_pointer.seek(offset)
        file_pointer.write(value_len.to_bytes(2, byteorder='big'))

        offset += 2
        file_pointer.seek(offset)
        file_pointer.write(key_as_bytes)

        offset += key_len

        byte_array = list(value_as_bytes)

        available_bytes_in_current_record = usable_bytes_in_a_record - key_len
        remaining_bytes_to_write = value_len

        starts_at = 0
        while True:

            bytes_to_write = bytes(byte_array[starts_at:starts_at + available_bytes_in_current_record])
            file_pointer.seek(offset)
            file_pointer.write(bytes_to_write)
            remaining_bytes_to_write = remaining_bytes_to_write - available_bytes_in_current_record

            starts_at = starts_at + available_bytes_in_current_record

            if remaining_bytes_to_write <= 0:
                break

            offset += available_bytes_in_current_record
            offset += 1

            file_pointer.seek(offset)
            file_pointer.write((2).to_bytes(1))

            available_bytes_in_current_record = usable_bytes_in_a_record

            offset += 8

        return slots_count

    @thread_safe
    def read(self,
             file_pointer,
             record_offset: int) -> Record:

        slot_count_pointer = record_offset + 1
        ttl_pointer = record_offset + 2
        key_len_pointer = record_offset + 6
        value_len_pointer = record_offset + 7
        key_value_bytes_starts_at = 9

        file_pointer.seek(record_offset)
        type_flag = int.from_bytes(file_pointer.read(1))

        file_pointer.seek(slot_count_pointer)
        slots_count = int.from_bytes(file_pointer.read(1))

        file_pointer.seek(ttl_pointer)
        ttl_seconds = int.from_bytes(file_pointer.read(4), 'big')

        file_pointer.seek(key_len_pointer)
        key_len_in_bytes = file_pointer.read(1)

        file_pointer.seek(value_len_pointer)
        val_len_in_bytes = file_pointer.read(2)

        key_len = int.from_bytes(key_len_in_bytes, 'big')
        val_len = int.from_bytes(val_len_in_bytes, 'big')

        key_pointer: int = record_offset + key_value_bytes_starts_at
        value_pointer: int = record_offset + key_value_bytes_starts_at + key_len

        file_pointer.seek(key_pointer)
        key_as_bytes = file_pointer.read(key_len)

        value_as_byte_list = []

        usable_bytes_in_a_record = self.slot_size_in_bytes - 5
        available_bytes_in_current_record = usable_bytes_in_a_record - key_len
        offset = value_pointer
        bytes_to_read = min(val_len, available_bytes_in_current_record)
        remaining_to_read = val_len

        while True:

            file_pointer.seek(offset)
            bytes_read = file_pointer.read(bytes_to_read)
            value_as_byte_list.extend(list(bytes_read))
            remaining_to_read = remaining_to_read - bytes_to_read

            if remaining_to_read <= 0:
                break

            offset += available_bytes_in_current_record
            offset += self.metadata_bytes_length_in_a_slot
            available_bytes_in_current_record = usable_bytes_in_a_record

            bytes_to_read = min(remaining_to_read,
                                available_bytes_in_current_record)

        return (type_flag,
                slots_count,
                ttl_seconds,
                key_len,
                val_len,
                key_as_bytes,
                bytes(value_as_byte_list))

    @thread_safe
    def is_available(self,
                     file_pointer,
                     record_offset: int):

        file_pointer.seek(record_offset)
        type_flag = int.from_bytes(file_pointer.read(1))

        if type_flag == 1:
            return True
        return False

    @thread_safe
    def delete(self,
               file_pointer: mmap,
               record_offset: int):

        slot_count_pointer = record_offset + 1

        file_pointer.seek(slot_count_pointer)
        slots_count = int.from_bytes(file_pointer.read(1))

        index = 0
        offset = record_offset
        while index < slots_count:
            file_pointer.seek(offset)
            file_pointer.write((0).to_bytes(1))
            offset += self.slot_size_in_bytes
            index += 1

    def get_slots_needed(self, key_len, value_len):
        slots_count = math.ceil((key_len + value_len) * 1.0 / self.usable_bytes_in_a_slot)
        return slots_count
