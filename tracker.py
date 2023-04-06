import asyncio
import sys
import logging
from collections import defaultdict
from utils import *
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
            logger.info(
                f"\n---\nfiles:{self.files}\npeers:{self.peers}\n---\n")
            self.transport.sendto(serialize({"status": "ok"}), addr)
        elif message["type"] == "get":
            filename, peer_id, peer_ip, peer_port = message["filename"], message[
                "peer_id"], message["peer_ip"], message["peer_port"]
            peers = self.files.get(filename)
            if peers:
                response = serialize({"status": "ok",
                                      "filename": filename,
                                      "n": len(peers),
                                      "peers": peers})
                self.transport.sendto(response, addr=addr)
                logging.info(f"{peer_id} requested {filename}, sent to {addr}")
            else:
                self.transport.sendto(
                    serialize({"status": "bad", "message": "file not found."}))
                logging.error(
                    f"{peer_id} requested {filename}, but file not found")
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
        await run_terminal(protocol)  # run until quit
    finally:
        transport.close()


async def run_tracker():
    if len(sys.argv) != ARG_LEN:
        raise ValueError("Error in arguments.")
    ip, port = split_addr(addr=sys.argv[1])
    await run_server(ip, port)


async def run_terminal(protocol):
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
            print(tail(file_path="tracker.log", n=n))
        if command_args[0] == "file_logs":
            if command_args[1] == "-all":
                print(protocol.files)
            else:
                print(protocol.files[command_args[1]])
        elif command_args[0] == "quit":
            return
        else:
            pass

if __name__ == "__main__":
    asyncio.run(run_tracker())
