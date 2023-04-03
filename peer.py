import asyncio
import asyncudp
import sys
from utils import split_addr

ARG_LEN = 5

async def run_client(ip, port):
    sock = await asyncudp.create_socket(remote_addr=("127.0.0.1", port))
    for _ in range(10):
        sock.sendto(b'get')
        print("set the get message")
        data, addr = await sock.recvfrom()
        print(f"got {data.decode()} from {addr}")
        await asyncio.sleep(0.5)
    sock.close()

if __name__ == "__main__":
    if len(sys.argv) != ARG_LEN:
        raise ValueError("Error in arguments.")
    if sys.argv[1] not in {"share", "get"}:
        raise ValueError(f"Unknown mode {sys.argv[1]}.")
    mode = sys.argv[1]
    filename = sys.argv[2]
    tracker_ip, tracker_port = split_addr(sys.argv[3])
    listen_ip, listen_port = split_addr(sys.argv[4])
    asyncio.run(run_client(tracker_ip, tracker_port))