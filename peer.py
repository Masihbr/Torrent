import asyncio
import sys
import logging
import random
from utils import *
from uuid import uuid4

from log import configure_logging

configure_logging('peer.log')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.propagate = False

PACKET_SIZE = 1024
BUFFER_SIZE = 512
peer_id = None
PING_TIMEOUT = 1
PING_INTERVAL = 10
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
        logger.debug(f"PeerShareProtocol message {message}")
        self.transport.sendto(serialize(message))

    def datagram_received(self, data, addr):
        response = deserialize(data)
        logger.debug(
            f"PeerShareProtocol response on Share {response} from {addr}")
        if response["status"] == "ok":
            logger.debug(f"{peer_id} shared {self.filename} successfully")
        else:
            logger.error(response)
        logger.debug("PeerShareProtocol closed the socket")
        self.transport.close()

    def error_received(self, exc):
        logger.error(f"PeerShareProtocol received error {exc}")

    def connection_lost(self, exc):
        logger.debug('PeerShareProtocol closed the connection')
        self.on_con_lost.set_result(True)


class PeerGetProtocol(asyncio.DatagramProtocol):
    def __init__(self, filename, peer_ip, peer_port, on_con_lost):
        self.filename = filename
        self.peer_ip = peer_ip
        self.peer_port = peer_port
        self.chosen_peer = None
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
        logger.debug(f"PeerGetProtocol message {message}")
        self.transport.sendto(serialize(message))

    def datagram_received(self, data, addr):
        response = deserialize(data)
        logger.debug(f"PeerGetProtocol response {response} from {addr}")
        if response["status"] == "ok":
            peer = random.choice(response["peers"])
            self.chosen_peer = peer
        else:
            logger.error(response)
        logger.debug("PeerGetProtocol closed the socket")
        self.transport.close()

    def error_received(self, exc):
        logger.error(f"Error received {exc}")

    def connection_lost(self, exc):
        logger.debug('PeerGetProtocol closed the connection')
        self.on_con_lost.set_result(True)


class PeerPingProtocol(asyncio.DatagramProtocol):
    def __init__(self, on_con_lost):
        self.on_con_lost = on_con_lost
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport
        message = serialize({"type": "ping", "peer_id": f"{peer_id}"})
        logger.info(f"PeerPingProtocol message {message}")
        self.transport.sendto(message)

    def datagram_received(self, data, addr):
        response = deserialize(data)
        logger.info(f"PeerPingProtocol response {response} from {addr}")
        if response["status"] == "ok":
            logger.info("Pinged tracker successfully.")
        else:
            logger.error("Failed to ping tracker.")
        logger.info("Close the Ping socket")
        self.transport.close()

    def error_received(self, exc):
        logger.error(f"PeerPingProtocol received error {exc}")

    def connection_lost(self, exc):
        logger.info('PeerPingProtocol closed the connection')
        self.on_con_lost.set_result(True)


async def download_file(peer: dict, filename: str) -> None:
    reader, writer = await asyncio.open_connection(host=peer["peer_ip"], port=peer["peer_port"])
    payload = serialize({"filename": filename, "peer_id": f"{peer_id}"})
    logger.debug(f"download_file payload {payload}")
    writer.write(payload)
    await writer.drain()
    break_out = False
    while True:
        chunk = await reader.read(PACKET_SIZE)
        logger.debug(f"download_file raw data len:{len(chunk)}")
        if not chunk:
            break
        with open(f"{filename}", mode="ab") as file:
            file.write(chunk)
    logger.debug(f"download_file from {peer} finished.")
    writer.close()
    await writer.wait_closed()


async def handle_file_share(reader, writer, filename):
    logger.debug(f"\n---\n{peer_id} server started.\n---\n")
    try:
        data = await reader.read(PACKET_SIZE)
        logger.debug(f"handle_file_share received payload {data}")
        message = deserialize(data)
        peer_name = writer.get_extra_info('peername')
        logger.debug(
            f'Client connected from {peer_name[0]}:{peer_name[1]} message:{message}')
        await writer.drain()
        try:
            requested_filename = message.get("filename", "")
            if requested_filename != filename:
                writer.write(
                    serialize({"status": "bad", "message": f"wrong file requested from {peer_id}."}))
                await writer.drain()
                return
            chunk_count = get_chunk_count(filename, buffer_size=BUFFER_SIZE)
            for chunk, index in read_binary_file_with_buffer(filename, buffer_size=BUFFER_SIZE):
                logger.debug(
                    f"handle_file_share sending to {peer_name} chunk {index} of {chunk_count}, chunk={len(chunk)}")
                writer.write(chunk)
            await writer.drain()
        except FileNotFoundError:
            writer.write(
                serialize({"status": "bad", "message": f"file not found on peer {peer_id}."}))
            await writer.drain()
        logger.debug(f"handle_file_share sending to {peer_name} finished!")
    except Exception as e:
        logger.error(f"handle_file_share closed the connection {e}")
    finally:
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
    logger.debug(
        f"{peer_id} notified tracker {tracker_ip}:{tracker_port} to share file {filename}")


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
        peer = file_protocol.chosen_peer
        if peer:
            await download_file(peer, filename)
        transport.close()
    logger.debug(f"{peer_id} got {filename} from {peer}")


async def share_file(filename, tracker_ip, tracker_port, peer_ip, peer_port):
    server = await asyncio.start_server(lambda reader, writer: handle_file_share(reader, writer, filename), host=peer_ip, port=peer_port)
    async with server:
        await notify_tracker(filename, tracker_ip, tracker_port, peer_ip, peer_port)
        await server.serve_forever()


async def run_client(mode, filename, tracker_ip, tracker_port, listen_ip, listen_port):
    if mode == "share":
        await share_file(filename=filename, tracker_ip=tracker_ip, tracker_port=tracker_port, peer_ip=listen_ip, peer_port=listen_port)
    elif mode == "get":
        await get_file(filename=filename, tracker_ip=tracker_ip, tracker_port=tracker_port, peer_ip=listen_ip, peer_port=listen_port)
        await share_file(filename=filename, tracker_ip=tracker_ip, tracker_port=tracker_port, peer_ip=listen_ip, peer_port=listen_port)


async def run_peer():
    if len(sys.argv) != ARG_LEN:
        raise ValueError("Error in arguments.")
    mode = sys.argv[1].lower()
    if mode not in {"share", "get"}:
        logger.error(f"Unknown mode {mode}.")
        raise ValueError(f"Unknown mode {mode}.")
    filename = sys.argv[2]
    tracker_ip, tracker_port = split_addr(sys.argv[3])
    listen_ip, listen_port = split_addr(sys.argv[4])
    global peer_id
    peer_id = uuid4()
    await asyncio.gather(run_client(mode, filename, tracker_ip,
                                    tracker_port, listen_ip, listen_port), ping_pong(tracker_ip, tracker_port))


async def run_terminal():
    while True:
        command = await get_input()
        command_args = list(map(str.lower, command.split()))
        if len(command_args) < 1:
            continue
        if command_args[0] == "tail":
            try:
                n = int(command_args[1])
            except:
                n = 50
            print(tail(file_path="peer.log", n=n))
        elif command_args[0] == "quit":
            for task in asyncio.all_tasks():
                task.cancel()
            return


async def ping_pong(tracker_ip, tracker_port):
    while True:
        on_con_lost = asyncio.get_running_loop().create_future()
        transport, protocol = await asyncio.get_event_loop().create_datagram_endpoint(
            lambda: PeerPingProtocol(on_con_lost=on_con_lost),
            remote_addr=(tracker_ip, tracker_port))
        try:
            await on_con_lost
        finally:
            transport.close()
        await asyncio.sleep(PING_INTERVAL)


async def main():
    await asyncio.gather(run_peer(), run_terminal())

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except asyncio.exceptions.CancelledError:
        print("Bye!")
