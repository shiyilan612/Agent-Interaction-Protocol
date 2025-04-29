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
    
    def __init__(self, 
                 tool_info: pb2.ToolInfo,
                 heartbeat_interval: int = 10,
                 update_check_interval: int = 10):
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
            heartbeat_interval: Interval in seconds between heartbeats
            update_check_interval: Interval in seconds between checking for self info updates
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
        
        # Heartbeat
        self._heartbeat_task = None
        self.heartbeat_interval = heartbeat_interval
        
        # Update check
        self._update_check_task = None
        self.update_check_interval = update_check_interval
        self._last_info_hash = self._hash_tool_info(self.tool_info) #hash of the history tool info for detecting changes

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
    
    
    async def _send_heartbeat(self):
        """Send periodic heartbeats to the gateway."""
        if not self._gateway_address:
            return
        stub = self._connection_pool.get_stub(self._gateway_address)
        
        while True:
            try:
                # Create heartbeat request
                request = pb2.HeartbeatRequest(sender_id=self.tool_id)
                # Send heartbeat
                response = await stub.Heartbeat(request)
                if not response.success:
                    print(f"<{self.tool_id}>: Heartbeat failed: {response.message}")
                else:
                    print(f"<{self.tool_id}>: Heartbeat sent successfully")
                    
                # Wait for the next interval
                await asyncio.sleep(self.heartbeat_interval)
                
            except grpc.aio.AioRpcError as e:
                print(f"<{self.tool_id}>: Heartbeat RPC error: {e.details()}")
                await asyncio.sleep(self.heartbeat_interval)
            except Exception as e:
                print(f"<{self.tool_id}>: Heartbeat error: {str(e)}")
                await asyncio.sleep(self.heartbeat_interval)

    def _hash_tool_info(self, tool_info: pb2.ToolInfo) -> int:
        """
        Create a hash of tool info for detecting changes.
        
        Args:
            tool_info: Tool information to hash
            
        Returns:
            Hash value as integer
        """
        hash_components = [
            tool_info.tool_id,
            tool_info.name,
            tool_info.domain,
            tool_info.description,
            tool_info.version,
            str(tool_info.input_mode),
            str(tool_info.output_mode),
            str({k: v for k, v in tool_info.arguments.items()})
        ]
        return hash("".join(hash_components))


    async def _check_tool_info_updates(self):
        """Periodically check if tool info has changed and notify gateway if needed."""
        if not self._gateway_address:
            return
        stub = self._connection_pool.get_stub(self._gateway_address)
        
        while True:
            try:
                # Check if tool info has changed
                current_hash = self._hash_tool_info(self.tool_info)
                if current_hash != self._last_info_hash:
                    print(f"<{self.tool_id}>: Tool info changed, updating gateway")
                    self._last_info_hash = current_hash
                    
                    # Create update request
                    peer = pb2.Peer()
                    peer.tool_info.CopyFrom(self.tool_info)
                    request = pb2.UpdateNodeInfoRequest(
                        sender_id=self.tool_id,
                        peer=peer
                    )
                    # Send update request to the gateway
                    response = await stub.UpdateNodeInfo(request)
                    if not response.success:
                        print(f"<{self.tool_id}>: Update failed: {response.message}")
                    else:
                        print(f"<{self.tool_id}>: Tool info updated successfully")
                
                # Wait for the next interval
                await asyncio.sleep(self.update_check_interval)
                
            except grpc.aio.AioRpcError as e:
                print(f"<{self.tool_id}>: Update check RPC error: {e.details()}")
                await asyncio.sleep(self.update_check_interval)
            except Exception as e:
                print(f"<{self.tool_id}>: Update check error: {str(e)}")
                await asyncio.sleep(self.update_check_interval)


            
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
        # Cancel all background tasks
        for task in [self._heartbeat_task, self._update_check_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        
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
            
            # Start background tasks after successful registration
            self._heartbeat_task = asyncio.create_task(self._send_heartbeat())
            self._update_check_task = asyncio.create_task(self._check_tool_info_updates())
            
            
        except grpc.aio.AioRpcError as e:
            print(f"RPC Error: {e.details()}")
            if e.code() == grpc.StatusCode.UNKNOWN:
                # Handle BrokenPipeError
                pass
        except Exception as e:
            print(f"Other exception: {str(e)}")