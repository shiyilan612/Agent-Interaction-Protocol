# -*- coding: utf-8 -*-
"""
Created on Fri Apr 18 10:01:08 2025

@author: xmkang
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Apr 17 22:11:16 2025

@author: xmkang
"""

# tool_service.py
import grpc
import asyncio
import uuid
# Import the generated proto modules
from .schema_pb2 import ToolInfo, ToolRequest, ToolResponse, Mode
from .schema_pb2_grpc import ToolServiceServicer, add_ToolServiceServicer_to_server
from .schema_pb2_grpc import GatewayServiceStub
from .utils import ConnectionPool
from typing import Dict

class ToolService(ToolServiceServicer):
    """Base ToolService class for handling tool requests and registration with gateway."""
    
    def __init__(self, 
                 tool_id: str = None,
                 name: str = None,
                 address: str = None,
                 gateway_address: str = None,
                 domain: str = "default",
                 description: str = "",
                 version: str = "1.0.0",
                 input_mode: Mode = Mode.TEXT,
                 output_mode: Mode = Mode.TEXT,
                 arguments: Dict[str, str] = None):
        """
        Initialize a new ToolService instance.
        
        Args:
            tool_id: Unique identifier for this tool (defaults to UUID if not provided)
            name: Human-readable name for this tool
            address: Address where this tool service will be hosted (e.g., "localhost:50051")
            gateway_address: Address of the gateway to register with (e.g., "localhost:50050")
            domain: Tool group/domain 
            description: Detailed description of the tool's functionality
            version: Tool version
            input_mode: Expected input modality (TEXT, IMAGE, etc.)
            output_mode: Output modality provided by the tool
        """
        self.tool_id = tool_id if tool_id else str(uuid.uuid4())
        self.name = name if name else self.tool_id
        self.address = address
        self.gateway_address = gateway_address
        self.domain = domain
        self.description = description
        self.version = version
        self.input_mode = input_mode
        self.output_mode = output_mode
        self.arguments = arguments
        
        #creat tool info
        self.tool_info = self._create_tool_info()
        
        # gRPC server for this tool service
        self._server = None
        #Stubs of nodes connected to this tool service
        self._connection_pool = ConnectionPool()

    async def process_tool_request(self, request: ToolRequest) -> ToolResponse:
        """子类需要实现CallTool消息处理逻辑"""
        raise NotImplementedError

        
    async def CallTool(self, request: ToolRequest, 
                 context: grpc.aio.ServicerContext) -> ToolResponse:
        """
        Handle incoming tool requests (implements the gRPC service method).
        
        Args:
            request: The incoming tool request
            context: The gRPC context
            
        Returns:
            ToolResponse containing the result or error
        """
        processed_msg = await self.process_tool_request(request)
        return processed_msg
            
    async def start(self) -> None:
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
        
        # Wait for termination
        try:
            await self._server.wait_for_termination()
        finally:
            # Ensure proper server shutdown
            await self._server.stop(1)  # 1 second timeout
            
    async def stop(self) -> None:
        """Stop the tool service gRPC server."""
        if self._server:
            await self._server.stop(grace=None)
            print(f"Tool service at {self.address} stopped")
        # Close all connections in the pool
        await self._connection_pool.close_all()
    
    def _create_tool_info(self) -> ToolInfo:
        """Create a ToolInfo message for registration with the gateway."""
        tool_info = ToolInfo(
            tool_id=self.tool_id,
            address=self.address,
            name=self.name,
            domain=self.domain,
            input_mode=self.input_mode,
            output_mode=self.output_mode,
            description=self.description,
            version=self.version,
            arguments=self.arguments
        )
                
        return tool_info

    async def connect_to_gateway(self, gateway_address: str):
        """
        Connect to the gateway service and register this tool.
        
        Args:
            gateway_address: The address of the gateway service
        """
        self.gateway_address = gateway_address
        await self._connection_pool.create_stub(self.gateway_address, GatewayServiceStub)
        stub = self._connection_pool.get_stub(self.gateway_address)
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
        
            
    
