# -*- coding: utf-8 -*-
"""
Created on Fri Apr 18 15:48:57 2025

@author: xmkang
"""
from session.tool_session import Tool
import asyncio
from grpc_service import schema_pb2, AgentService, GatewayService

async def main():
    gw_local = GatewayService(gw_id="gw_local")
    asyncio.create_task(gw_local.start(port=50051))
    await asyncio.sleep(10)
    
    
    # Example: Creating a function-based tool
    async def calculate_sum(a:int, b:int) -> int:
        """Adds two numbers and returns the sum."""
        return int(a) + int(b)

    sum_tool = Tool.creat_function_tool(
        function=calculate_sum,
        address="localhost:50054",
        gateway_address="localhost:50051",
        name="SumCalculator",
        description="A simple tool that adds two numbers"
    )
    
    tool_task = asyncio.create_task(sum_tool.run())
    await asyncio.sleep(10)
    
    #direct invoke without agent and gateway
    response = await sum_tool.invoke_direct(
            arguments = {"a":"5", "b":"3"}
            )
    print(response.text)


if __name__ == "__main__":
    # run main process
    asyncio.get_event_loop().set_debug(True)
    asyncio.run(main())
