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
READ_SIZE = 1024
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

    def connection_lost(self, exc):
        logger.info('The server closed the connection')
        self.on_con_lost.set_result(True)


class PeerGetProtocol(asyncio.DatagramProtocol):
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
            "type": "get",
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
            logger.info(f"Got {response} successfully")
        else:
            logger.error(response)
        logger.info("Close the socket")
        self.transport.close()

    def error_received(self, exc):
        logger.error(f"Error received {exc}")

    def connection_lost(self, exc):
        logger.info('The server closed the connection')
        self.on_con_lost.set_result(True)


async def handle_file_share(reader, writer):
    logger.info(f"\n---\n{peer_id} server started.\n---\n")
    try:
        while True:
            data = await reader.read(READ_SIZE)
            message = data.decode()
            addr = writer.get_extra_info('peername')
            logger.info(f"Received {message!r} from {addr!r}")
            logger.info(f"Send: {message!r}")
            writer.write(data)
            await writer.drain()
    except:
        logger.info("Close the connection")
        writer.close()
        await writer.wait_closed()


async def notify_tracker(filename, tracker_ip, tracker_port, peer_ip, peer_port):
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


async def get_file(filename, tracker_ip, tracker_port, peer_ip, peer_port):
    on_con_lost = asyncio.get_running_loop().create_future()
    transport, file_protocol = await asyncio.get_running_loop().create_datagram_endpoint(
        lambda: PeerGetProtocol(
            filename, peer_ip, peer_port, on_con_lost=on_con_lost),
        remote_addr=(tracker_ip, tracker_port)
    )
    try:
        await on_con_lost
    finally:
        transport.close()
    logger.info(f"{peer_id} got {filename}")


async def share_file(filename, tracker_ip, tracker_port, peer_ip, peer_port):
    server = await asyncio.start_server(handle_file_share, host=peer_ip, port=peer_port)
    async with server:
        await notify_tracker(filename, tracker_ip, tracker_port, peer_ip, peer_port)
        await server.serve_forever()


async def run_client(mode, filename, tracker_ip, tracker_port, listen_ip, listen_port):
    if mode == "share":
        await share_file(filename=filename, tracker_ip=tracker_ip, tracker_port=tracker_port, peer_ip=listen_ip, peer_port=listen_port)
    elif mode == "get":
        await get_file(filename=filename, tracker_ip=tracker_ip, tracker_port=tracker_port, peer_ip=listen_ip, peer_port=listen_port)


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
