# -*- coding: utf-8 -*-
"""
Created on Fri Apr 18 10:01:08 2025

@author: xmkang
"""

# -*- coding: utf-8 -*-

import grpc
import asyncio
from .utils import ConnectionPool

# Import the generated proto modules
from .schema_pb2_grpc import ToolServiceServicer, add_ToolServiceServicer_to_server
from .schema_pb2_grpc import GatewayServiceStub
from . import schema_pb2 as pb2


class ToolService(ToolServiceServicer):
    """Base ToolService class for handling tool requests and registration with gateway."""
    
    def __init__(self, tool_info: pb2.ToolInfo):
        """
        Initialize a new Tool instance.

        Args:
            tool_info:
                 address: str,
                 tool_id: str = None,
                 name: str = None,
                 domain: str = "default",
                 description: str = "",
                 version: str = "1.0.0",
                 input_mode: pb2.Mode = pb2.Mode.TEXT,
                 output_mode: pb2.Mode = pb2.Mode.TEXT,
                 arguments: Dict[str, str] = None
        """
        self.tool_info = tool_info
        self.tool_id = self.tool_info.tool_id
        self.address = self.tool_info.address

        # init gateway address
        self._gateway_address = None

        # gRPC server for this tool service
        self._server = None

        # stubs of nodes connected to this tool service
        self._connection_pool = ConnectionPool()

    async def _handle_server_termination(self):
        try:
            await self._server.wait_for_termination()
        except Exception as e:
            print(f"Error during server termination: {e}")
        finally:
            await self._server.stop(1)

    async def CallTool(self, request: pb2.ToolRequest,
                 context: grpc.aio.ServicerContext) -> pb2.ToolResponse:
        """
        Handle incoming tool requests (implements the gRPC service method).
        
        Args:
            request: The incoming tool request
            context: The gRPC context
            
        Returns:
            ToolResponse containing the result or error
        """
        pass
            
    async def start(self):
        """
        Start the tool service gRPC server.
        
        Args:
            port: Port number to listen on
        """
        self._server = grpc.aio.server()
        add_ToolServiceServicer_to_server(self, self._server)
        self._server.add_insecure_port(self.address)
        await self._server.start()
        print(f"<{self.tool_id}>: Tool {self.tool_id} started on {self.address}")
        asyncio.create_task(self._handle_server_termination())

        return self
            
    async def stop(self) -> None:
        """Stop the tool service gRPC server."""
        if self._server:
            await self._server.stop(grace=None)
            print(f"Tool service at {self.address} stopped")
        # Close all connections in the pool
        await self._connection_pool.close_all()

    async def connect_to_gateway(self, gateway_address: str):
        """
        Connect to the gateway service and register this tool.
        
        Args:
            gateway_address: Address of the gateway to register with
        """
        self._gateway_address = gateway_address
        await self._connection_pool.create_stub(self._gateway_address, GatewayServiceStub)
        stub = self._connection_pool.get_stub(self._gateway_address)
        try:
            # Register Tool with gateway
            response = await stub.RegisterTool(self.tool_info)
            print(f"<{self.tool_id}>: RegisterResponse from Gateway ({gateway_address})")
        except grpc.aio.AioRpcError as e:
            print(f"RPC Error: {e.details()}")
            if e.code() == grpc.StatusCode.UNKNOWN:
                # Handle BrokenPipeError
                pass
        except Exception as e:
            print(f"Other exception: {str(e)}")