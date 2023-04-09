import ipaddress
import json
import os
import math
from typing import Tuple
import asyncio
import time


class ExpiringSet(set):
    def __init__(self):
        self.expiry_times = {}

    def add(self, value, timeout=10):
        super().add(value)
        self.expiry_times[value] = time.time() + timeout

    def remove_expired(self):
        now = time.time()
        expired_values = [
            value for value, expiry_time in self.expiry_times.items() if now >= expiry_time]
        for value in expired_values:
            super().remove(value)
            del self.expiry_times[value]

    def __iter__(self):
        self.remove_expired()
        return super().__iter__()

    def __contains__(self, value):
        self.remove_expired()
        return super().__contains__(value)

    def __len__(self):
        self.remove_expired()
        return super().__len__()


def validate_addr(ip: str, port: str) -> None:
    ipaddress.ip_address(ip)
    if not 0 < int(port) < 65535:
        raise ValueError("Invalid port.")


def split_addr(addr: str) -> Tuple[str, str]:
    ip, port = addr.split(":")
    validate_addr(ip, port)
    return ip, port


class BinaryJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, bytes):
            return {'__binary__': True, 'data': list(obj)}
        return super().default(obj)


class BinaryJSONDecoder(json.JSONDecoder):
    def __init__(self, *args, **kwargs):
        super().__init__(object_hook=self.object_hook, *args, **kwargs)

    def object_hook(self, dct):
        if '__binary__' in dct:
            return bytes(dct['data'])
        return dct


def serialize(data: dict) -> bytes:
    return json.dumps(data, cls=BinaryJSONEncoder).encode()


def deserialize(data: bytes) -> dict:
    try:
        return json.loads(data.decode().strip(), cls=BinaryJSONDecoder)
    except Exception as e:
        return {"err": True, "message": f"Error in deserialization: {e}"}


def tail(file_path, n):
    with open(file_path, "r") as f:
        lines = f.read().splitlines()
        lines = list(filter(lambda x: "- root - INFO -" not in x, lines))
    return "\n".join(lines[-n:])


async def get_input(input_str: str = "> "):
    return await asyncio.get_event_loop().run_in_executor(None, input, input_str)


def read_binary_file_with_buffer(file_path, buffer_size=512):
    with open(file_path, 'rb') as file:
        buffer = file.read(buffer_size)
        part_index = 0
        while buffer:
            yield (buffer, part_index)
            part_index += 1
            buffer = file.read(buffer_size)


def get_chunk_count(file_path, buffer_size):
    file_size = os.path.getsize(file_path)
    num_parts = math.ceil(file_size / buffer_size)
    return num_parts
