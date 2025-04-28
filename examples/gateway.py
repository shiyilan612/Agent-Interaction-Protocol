import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import asyncio
from module import Gateway

async def main():
    # Create and start Gateway
    gateway = Gateway(address="localhost:50050")
    await gateway.start()
    print("Gateway started")
    
    await asyncio.sleep(9999999)
    
if __name__ == "__main__":
    # run main process
    asyncio.get_event_loop().set_debug(True)
    asyncio.run(main())
