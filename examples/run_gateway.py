# run_gateway.py
import asyncio
import argparse
from atlink_aip.module import Gateway


async def main(args):
    gateway = Gateway(host_address=args.host_address, gateway_id=args.gateway_id)
    await gateway.start()

    try:
        await asyncio.sleep(float('inf'))
    except asyncio.CancelledError:
        await gateway.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--host_address", default="localhost:50000")
    parser.add_argument("--gateway_id", default="example_gateway")
    args = parser.parse_args()

    asyncio.run(main(args))
