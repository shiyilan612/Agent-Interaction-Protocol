# run_gateway.py
import asyncio
from atlink.module import Gateway

async def main():
    gateway = Gateway(address="localhost:50050", gateway_id="test_gw")
    await gateway.start()
    print("Gateway started. Press Ctrl+C to exit.")
    try:
        while True:
            await asyncio.sleep(9999999)
    except asyncio.CancelledError:
        print("Stopping gateway...")
        await gateway.stop()

if __name__ == "__main__":
    asyncio.run(main())
