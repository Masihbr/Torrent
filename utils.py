import ipaddress
from typing import Tuple

def validate_addr(ip: str, port: str) -> None:
    ipaddress.ip_address(ip)
    if not 0 < int(port) < 65535:
        raise ValueError("Invalid port.")

def split_addr(addr: str) -> Tuple[str, str]:
    ip, port = addr.split(":")
    validate_addr(ip, port)
    return ip, port