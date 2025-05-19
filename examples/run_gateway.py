# run_gateway.py
import sys
import os
import asyncio
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from module import Gateway

async def main():
    gateway = Gateway(address="localhost:50050", gateway_id="test_gw")
    await gateway.start()
    print("Gateway started. Press Ctrl+C to exit.")
    while True:
        await asyncio.sleep(9999999)

if __name__ == "__main__":
    asyncio.run(main())
