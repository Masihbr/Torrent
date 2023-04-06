import asyncio
import sys
import logging
from collections import defaultdict
from utils import split_addr, serialize, deserialize
from log import configure_logging

configure_logging('tracker.log')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.propagate = False

ARG_LEN = 2


class TrackerUDPServer(asyncio.DatagramProtocol):
    def __init__(self):
        self.files = defaultdict(list)
        self.peers = defaultdict(set)

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        message = deserialize(data)
        logger.info(f"got {message} from {addr}")
        if message["type"] == "share":
            filename, peer_id, peer_ip, peer_port = message["filename"], message[
                "peer_id"], message["peer_ip"], message["peer_port"]
            self.files[filename].append({
                "peer_id": peer_id,
                "peer_ip": peer_ip,
                "peer_port": peer_port
            })
            self.peers[peer_id].add(filename)
            logger.info(f"{peer_id} shared {filename}")
            logger.info(f"files:{self.files}\npeers:{self.peers}")
            self.transport.sendto(serialize({"status": "ok"}), addr)
        elif message.strip().startswith("get"):
            self.transport.sendto(serialize({"status": "bad"}), addr)

    def connection_lost(self, exc) -> None:
        logger.error(f"Connection lost {exc}")


async def run_server(ip: str, port: str):
    loop = asyncio.get_running_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: TrackerUDPServer(),
        local_addr=(ip, port)
    )
    logger.info(f"Listening on {ip}:{port}")

    try:
        await asyncio.sleep(3600)  # Serve for 1 hour.
    finally:
        transport.close()


if __name__ == "__main__":
    if len(sys.argv) != ARG_LEN:
        raise ValueError("Error in arguments.")
    ip, port = split_addr(addr=sys.argv[1])
    asyncio.run(run_server(ip, port))
