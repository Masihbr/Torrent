import ipaddress
import json
from typing import Tuple


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
    return json.loads(data.decode(), cls=BinaryJSONDecoder)
