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

    def connection_lost(self, exc):
        logger.info('The server closed the connection')
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
        logger.info(f"Send {message}")
        self.transport.sendto(serialize(message))

    def datagram_received(self, data, addr):
        response = deserialize(data)
        logger.info(f"Received {response} from {addr}")
        if response["status"] == "ok":
            peer = random.choice(response["peers"])
            self.chosen_peer = peer
        else:
            logger.error(response)
        logger.info("Close the socket")
        self.transport.close()

    def error_received(self, exc):
        logger.error(f"Error received {exc}")

    def connection_lost(self, exc):
        logger.info('The server closed the connection')
        self.on_con_lost.set_result(True)


async def download_file(peer: dict, filename: str) -> None:
    reader, writer = await asyncio.open_connection(host=peer["peer_ip"], port=peer["peer_port"])
    payload = serialize({"filename": filename})
    logger.info(f"Send payload {payload}")
    writer.write(payload)
    file_response = deserialize(await reader.read(1024))
    logger.info(f"file_response: {file_response}")
    if file_response["status"] == "ok":
        with open(f"{filename}", "wb") as file:
            file.write(file_response["contents"])
    else:
        logger.error(f"file not found.")
    writer.close()
    await writer.wait_closed()


async def handle_file_share(reader, writer):
    logger.info(f"\n---\n{peer_id} server started.\n---\n")
    try:
        while True:
            data = await reader.read(1024)
            logger.info(f"received payload {data}")
            message = deserialize(data)
            peer_name = writer.get_extra_info('peername')
            logger.info(
                f'Client connected from {peer_name[0]}:{peer_name[1]} message:{message}')
            await writer.drain()
            try:
                with open(message["filename"], 'rb') as f:
                    contents = f.read()
                    writer.write(
                        serialize({"status": "ok", "contents": contents}))
                    await writer.drain()
            except FileNotFoundError:
                writer.write(
                    serialize({"status": "bad", "message": "file not found."}))
                await writer.drain()
    except Exception as e:
        logger.error(f"Close the connection {e}")
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
        peer = file_protocol.chosen_peer
        if peer:
            await download_file(peer, filename)
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
        await share_file(filename=filename, tracker_ip=tracker_ip, tracker_port=tracker_port, peer_ip=listen_ip, peer_port=listen_port)


async def run_peer():
    if len(sys.argv) != ARG_LEN:
        raise ValueError("Error in arguments.")
    mode = sys.argv[1].lower()
    if mode not in {"share", "get"}:
        logger.error(f"Unknown Mode {mode}.")
        raise ValueError(f"Unknown Mode {mode}.")
    filename = sys.argv[2]
    tracker_ip, tracker_port = split_addr(sys.argv[3])
    listen_ip, listen_port = split_addr(sys.argv[4])
    global peer_id
    peer_id = uuid4()
    await run_client(mode, filename, tracker_ip,
                     tracker_port, listen_ip, listen_port)


def tail(file_path, n):
    with open(file_path, "r") as f:
        lines = f.read().splitlines()
    return "\n".join(lines[-n:])


async def get_input():
    return await asyncio.get_event_loop().run_in_executor(None, input, "> ")


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
            print("Do a ctrl-c to kill server!")
            return
        else:
            pass


async def main():
    await asyncio.gather(run_peer(), run_terminal())

if __name__ == "__main__":
    asyncio.run(main())
