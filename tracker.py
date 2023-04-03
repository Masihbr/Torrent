import asyncio
import sys
from utils import split_addr

ARG_LEN = 2

class CounterUDPServer:
    def __init__(self):
        self.counter = 0

    async def send_counter(self, addr):
        self.counter += 1
        next_value = self.counter
        await asyncio.sleep(0.5)
        print(f"sending {next_value} to {addr}")
        self.transport.sendto(str(next_value).encode(), addr)

    def connection_made(self, transport):
        self.transport = transport

    def datagram_received(self, data, addr):
        print(f"got {data.decode()} from {addr}")
        if data.decode().strip() != "get":
            return
        loop = asyncio.get_event_loop()
        loop.create_task(self.send_counter(addr))


async def run_server(ip: str, port: str):
    loop = asyncio.get_running_loop()
    await loop.create_datagram_endpoint(
        lambda: CounterUDPServer(),
        local_addr=(ip, port)
    )
    print(f"Listening on 127.0.0.1:{port}")
    while True:
        await asyncio.sleep(3600)


if __name__ == "__main__":
    if len(sys.argv) != ARG_LEN:
        raise ValueError("Error in arguments.")
    ip, port = split_addr(addr=sys.argv[1])
    asyncio.run(run_server(ip, port))
