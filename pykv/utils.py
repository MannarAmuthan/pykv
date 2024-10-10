import mmap
import os
from datetime import datetime, timezone, timedelta


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


def is_passed(timestamp: int):
    given_time = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    current_time = datetime.now(timezone.utc)
    return current_time > given_time


def add_seconds(input_time: datetime):
    return input_time + timedelta(0, 3)


def get_timestamp(input_time: datetime):
    return int(input_time.timestamp())
