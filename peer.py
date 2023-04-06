import asyncio
import sys
import logging
from utils import split_addr, serialize, deserialize
from uuid import uuid4

from log import configure_logging

configure_logging('peer.log')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.propagate = False

peer_id = None

ARG_LEN = 5

class PeerShareProtocol(asyncio.DatagramProtocol):
    def __init__(self, filename, peer_ip, peer_port, on_con_lost):
        self.filename = filename
        self.peer_ip = peer_ip
        self.peer_port = peer_port
        self.on_con_lost = on_con_lost
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport
        message = {
            "filename": self.filename,
            "type": "share",
            "peer_id": f"{peer_id}",
            "peer_ip": f"{self.peer_ip}",
            "peer_port": f"{self.peer_port}"
        }
        logger.info(f"Send {message}")
        self.transport.sendto(serialize(message))

    def datagram_received(self, data, addr):
        response = deserialize(data)
        logger.info(f"Received {response} from {addr}")
        if response["status"] == "ok":
            logger.info(f"{peer_id} shared {self.filename} successfully")
        else:
            logger.error(response)
        logger.info("Close the socket")
        self.transport.close()

    def error_received(self, exc):
        logger.error(f"Error received {exc}")


async def share_file(filename, tracker_ip, tracker_port, peer_ip, peer_port):
    on_con_lost = asyncio.get_running_loop().create_future()
    transport, file_protocol = await asyncio.get_running_loop().create_datagram_endpoint(
        lambda: PeerShareProtocol(
            filename, peer_ip, peer_port, on_con_lost=on_con_lost),
        remote_addr=(tracker_ip, tracker_port)
    )
    try:
        await on_con_lost
    finally:
        transport.close()
    logger.info(f"{peer_id} shared {filename}")


async def run_client(mode, filename, tracker_ip, tracker_port, listen_ip, listen_port):
    if mode == "share":
        await share_file(filename=filename, tracker_ip=tracker_ip, tracker_port=tracker_port, peer_ip=listen_ip, peer_port=listen_port)
    else:
        pass

if __name__ == "__main__":
    if len(sys.argv) != ARG_LEN:
        raise ValueError("Error in arguments.")
    mode = sys.argv[1].lower()
    if mode not in {"share", "get"}:
        logger.error(f"Unknown Mode {mode}.")
        raise ValueError(f"Unknown Mode {mode}.")
    filename = sys.argv[2]
    tracker_ip, tracker_port = split_addr(sys.argv[3])
    listen_ip, listen_port = split_addr(sys.argv[4])
    peer_id = uuid4()
    asyncio.run(run_client(mode, filename, tracker_ip,
                tracker_port, listen_ip, listen_port))
