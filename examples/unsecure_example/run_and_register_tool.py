import asyncio
import argparse
from atlink_aip.module import ToolBox

async def main(args):
    #init toolbox
    toolbox = ToolBox(
        host_address=args.host_address,
        name=args.toolbox_name,
        toolbox_id=args.toolbox_id
    )
    
    await toolbox.start()
    
    #add tool
    @toolbox.tool()
    async def calculate_sum(a: int, b: int) -> int:
        """Adds two numbers and returns the sum."""
        return int(a) + int(b)

    await toolbox.register_to_gateway(args.gateway_address)

    try:
        await asyncio.sleep(float('inf'))
    except asyncio.CancelledError:
        await toolbox.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway_address", default="localhost:50000")
    parser.add_argument("--host_address", default="localhost:51000")
    parser.add_argument("--toolbox_name", default="Math Box")
    parser.add_argument("--toolbox_id", default="example_tool")
    args = parser.parse_args()

    asyncio.run(main(args))
