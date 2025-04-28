import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import asyncio
from module import Tool

# Example: Creating a function-based tool
async def calculate_sum(a:int, b:int) -> int:
    """Adds two numbers and returns the sum."""
    return int(a) + int(b)

async def main():
    
    sum_tool = Tool.creat_function_tool(
        address="localhost:50051",
        function=calculate_sum,
        name="Sum",
        tool_id="tool1",
        description="A simple tool that adds two numbers"
    )
    
    await sum_tool.start()
    await sum_tool.register_to_gateway("localhost:50050")
    print("Tool registered")
    
    await asyncio.sleep(9999999)

if __name__ == "__main__":
    # run main process
    asyncio.get_event_loop().set_debug(True)
    asyncio.run(main())
