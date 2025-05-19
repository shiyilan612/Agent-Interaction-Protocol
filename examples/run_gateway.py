# run_gateway.py
import asyncio
from module import Gateway

async def main():
    gateway = Gateway(address="localhost:50000", gateway_id="test_gw")
    await gateway.start()
    print("Gateway started. Press Ctrl+C to exit.")
    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
