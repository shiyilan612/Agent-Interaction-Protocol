# -*- coding: utf-8 -*-
"""
Created on Fri Apr 18 10:59:48 2025

@author: xmkang
"""
import inspect
import asyncio
import functools
from typing import Dict, Any, Callable
from grpc_service import ToolService, GatewayService
from grpc_service.schema_pb2 import ToolRequest, ToolResponse, Mode


class Tool(ToolService):
    """
    Tool class that inherits from ToolService, providing a higher-level interface
    for creating and registering tools with the gateway.
    """

    def __init__(self,
                 tool_id: str = None,
                 name: str = None,
                 address: str = None,
                 domain: str = "default",
                 description: str = "",
                 version: str = "1.0.0",
                 input_mode: Mode = Mode.TEXT,
                 output_mode: Mode = Mode.TEXT,
                 handler: Callable = None,
                 arguments: Dict[str, str] = None):
        """
        Initialize a new Tool instance.

        Args:
            tool_id: Unique identifier for this tool
            name: Human-readable name for this tool
            address: Address where this tool service will be hosted (e.g., "localhost:50051")
            domain: Tool group/domain
            description: Detailed description of the tool's functionality
            version: Tool version
            input_mode: Expected input modality (TEXT, IMAGE, etc.)
            output_mode: Output modality provided by the tool
        """
        super().__init__(
            tool_id=tool_id,
            name=name,
            address=address,
            domain=domain,
            description=description,
            version=version,
            input_mode=input_mode,
            output_mode=output_mode,
            arguments=arguments
        )
        self._handler = handler

    async def process_tool_request(self, request: ToolRequest) -> ToolResponse:
        """
        Process an incoming tool request based on the tool's configuration.

        Args:
            request: The incoming tool request

        Returns:
            ToolResponse containing the result or error
        """
        assert (request.receiver_id == self.tool_id)
        try:
            # Create response template
            response = ToolResponse(
                sender_id=self.tool_id,
                receiver_id=request.sender_id,
                success=False
            )

            # Process based on tool type
            if self._handler is not None:
                # Function-based tool
                result = await self._execute_handler(request.arguments)
                response.text = str(result)
                response.success = True

            # elif self._api_config is not None:
            #     # API-based tool
            #     result = await self._execute_api_call(request.arguments)
            #     response.text = result
            #     response.success = True

            # elif self._mcp_config is not None:
            #     # MCP-based tool
            #     result = await self._execute_mcp_request(request.arguments)
            #     response.text = result
            #     response.success = True

            # else:
            #     # No handler defined
            #     response.error_message = "No handler configured for this tool"

            return response

        except Exception as e:
            # Handle any exceptions during processing
            return ToolResponse(
                sender_id=self.tool_id,
                receiver_id=request.sender_id,
                success=False,
                error_message=f"Error executing tool: {str(e)}"
            )

    async def _execute_handler(self, arguments: Dict[str, str]) -> Any:
        """Execute the function-based tool handler."""
        if not callable(self._handler):
            raise ValueError("Handler is not callable")

        # Check if handler is a coroutine function
        if inspect.iscoroutinefunction(self._handler):
            result = await self._handler(**arguments)
        else:
            # Run synchronous functions in the executor
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                functools.partial(self._handler, **arguments)
            )

        return result

    @classmethod
    def creat_function_tool(cls,
                            function: Callable,
                            address: str,
                            tool_id: str = None,
                            name: str = None,
                            domain: str = "default",
                            description: str = None,
                            version: str = "1.0.0",
                            input_mode: Mode = Mode.TEXT,
                            output_mode: Mode = Mode.TEXT,
                            arguments: Dict[str, str] = None) -> 'Tool':
        """
        Create a Tool from a Python function.

        Args:
            function: The function to wrap as a tool
            address: Address where this tool service will be hosted
            tool_id: Unique identifier (defaults to function name if not provided)
            name: Human-readable name (defaults to function name if not provided)
            domain: Tool domain/group
            description: Tool description (defaults to function docstring if not provided)
            version: Tool version
            input_mode: Input modality
            output_mode: Output modality
            arguments: Optional custom descriptions for the function arguments

        Returns:
            An initialized Tool instance
        """
        # Use function name as default tool ID and name
        if not tool_id:
            tool_id = function.__name__
        if not name:
            name = function.__name__

        # Use function docstring as default description
        if not description and function.__doc__:
            description = inspect.cleandoc(function.__doc__)
        elif not description:
            description = f"Tool based on function {function.__name__}"

        # Extract function signature to get arguments
        sig = inspect.signature(function)
        # If custom argument descriptions are not provided, generate them from the signature
        if not arguments:
            arguments = {
                param_name: str(param.annotation) if param.annotation != inspect.Parameter.empty else "any"
                for param_name, param in sig.parameters.items()
            }
        else:
            # Ensure all function parameters are included in the arguments dictionary
            for param_name in sig.parameters:
                if param_name not in arguments:
                    param = sig.parameters[param_name]
                    arguments[param_name] = str(
                        param.annotation) if param.annotation != inspect.Parameter.empty else "any"

        # Create the Tool instance
        tool = cls(
            tool_id=tool_id,
            name=name,
            address=address,
            domain=domain,
            description=description,
            version=version,
            input_mode=input_mode,
            output_mode=output_mode,
            handler=function,
            arguments=arguments
        )

        return tool

    async def run(self, gateway_address: str = None) -> None:
        """
        Run the tool service, registering with the gateway and starting the server.
        gateway_address: Address of the gateway to register with
        """
        print(f"Starting tool {self.name} ({self.tool_id})...")

        # First start the server
        server_task = asyncio.create_task(self.start())

        if gateway_address:
            self._gateway_address = gateway_address

        if self._gateway_address:
            await asyncio.sleep(1)  # Brief delay to ensure server is running
            await self.connect_to_gateway(self._gateway_address)
            print(f"Tool {self.name} registered with gateway at {self._gateway_address}")

        # Wait for server to complete
        await server_task

    async def invoke_direct(self, arguments: Dict[str, str] = None) -> ToolResponse:
        """
        invoke this tool directly without gateway
        """
        if not arguments:
            arguments = {}

        # create a direct request
        request = ToolRequest(
            sender_id="direct_caller",
            receiver_id=self.tool_id,
            tool_name=self.name,
            arguments=arguments
        )

        return await self.process_tool_request(request)


async def main():
    gw_local = GatewayService(address="localhost:50051", gw_id="gw_local")
    asyncio.create_task(gw_local.start())
    await asyncio.sleep(10)
    
    
    # Example: Creating a function-based tool
    async def calculate_sum(a:int, b:int) -> int:
        """Adds two numbers and returns the sum."""
        return int(a) + int(b)

    sum_tool = Tool.creat_function_tool(
        address="localhost:50054",
        function=calculate_sum,
        name="SumCalculator",
        description="A simple tool that adds two numbers"
    )
    
    asyncio.create_task(sum_tool.run())
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
