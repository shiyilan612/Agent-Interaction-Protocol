# -*- coding: utf-8 -*-
"""
Created on Fri Apr 18 10:01:08 2025

@author: xmkang & clleng
"""

# -*- coding: utf-8 -*-

import grpc
import asyncio
from .utils import ConnectionPool
from ..logger import LoggerManager

# Import the generated proto modules
from .schema_pb2_grpc import ToolServiceServicer, add_ToolServiceServicer_to_server
from .schema_pb2_grpc import GatewayServiceStub
from . import schema_pb2 as pb2


class ToolService(ToolServiceServicer):
    """Base ToolService class for handling tool requests and registration with gateway."""
    
    def __init__(self,
                 toolbox_info: pb2.ToolBoxInfo,
                 heartbeat_interval: int = 10):
        """
        Initialize a new Tool instance.

        Args:
            toolbox_info: 
                 address: str,
                 toolbox_id: str = None,
                 name: str = None,
                 domain: str = "default",
                 description: str = "",
                 tools: List[ToolInfo] = []
        """
        self.toolbox_info = toolbox_info
        self.toolbox_id = self.toolbox_info.toolbox_id
        self.address = self.toolbox_info.address

        # init gateway address
        self._gateway_address = None

        # gRPC server for this tool service
        self._server = None
        
        self._logger_mgr = LoggerManager()
        self._logger = self._logger_mgr.get_logger(self.toolbox_id)

        # stubs of nodes connected to this tool service
        self._connection_pool = ConnectionPool(self._logger)

        # Heartbeat
        self._heartbeat_task = None
        self.heartbeat_interval = heartbeat_interval

        
    async def _handle_server_termination(self):
        try:
            await self._server.wait_for_termination()
        except Exception as e:
            self._logger.error(f"<Tool>: Failed to terminate gRPC server - Exception: {e}")
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
        heartbeat_failed_count = 0

        while True:
            try:
                # Create heartbeat request
                request = pb2.HeartbeatRequest(sender_id=self.toolbox_id)
                # Send heartbeat
                response = await stub.Heartbeat(request)
                if not response.success:
                    heartbeat_failed_count += 1
                    self._logger.error(f"<Tool>: Heartbeat failed: {response.message}")
                else:
                    heartbeat_failed_count = 0
                    self._logger.debug(f"<Tool>: Heartbeat sent successfully")

                # Wait for the next interval
                await asyncio.sleep(self.heartbeat_interval)

            except grpc.aio.AioRpcError as e:
                heartbeat_failed_count += 1
                self._logger.error(f"<Tool>: Heartbeat RPC error: {e.details()}")
                await asyncio.sleep(self.heartbeat_interval)
            except Exception as e:
                heartbeat_failed_count += 1
                self._logger.error(f"<Tool>: Heartbeat error: {str(e)}")
                await asyncio.sleep(self.heartbeat_interval)

            if heartbeat_failed_count >= 3:
                self._logger.error(f"<Tool>: Heartbeat failed 3 times, disconnecting from gateway")
                await self.disconnect_from_gateway()
                break
            
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
        self._logger.info(f"<Tool>: ToolBox [{self.toolbox_id}] started on [{self.address}]")
        asyncio.create_task(self._handle_server_termination())

        return self
            
    async def stop(self) -> None:
        """Stop the tool service gRPC server."""
        if self._server:
            await self._server.stop(grace=10.0)
            await self.disconnect_from_gateway()
            self._logger.info(f"<Tool>: ToolBox service [{self.toolbox_id}] at [{self.address}] stopped")
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
            response = await stub.RegisterTool(self.toolbox_info)
            
            self._logger.info(f"<Tool>: {'Registered' if response else 'Failed to register'}"
                              f" to Gateway at [{gateway_address}]")

            self._heartbeat_task = asyncio.create_task(self._send_heartbeat())
            
        except grpc.aio.AioRpcError as e:
            self._logger.error(f"<Tool>: Failed to register to Gateway at [{gateway_address}] - "
                               f"RPC Error: {e.code()}, details: {e.details()}")
            if e.code() == grpc.StatusCode.UNKNOWN:
                # Handle BrokenPipeError
                pass
            raise
        except Exception as e:
            self._logger.error(f"<Tool>: Failed to register to Gateway at [{gateway_address}] - "
                               f"Exception: {str(e)}")
            
    async def disconnect_from_gateway(self):
        """
        Disconnect from the gateway service and deregister this Tool.
        """
        stub = self._connection_pool.get_stub(self._gateway_address)

        try:
            # deregister tool  
            response = await stub.DeregisterNode(pb2.DeregisterNodeRequest(node_id=self.toolbox_id))          
            await self._connection_pool.close_stub(self._gateway_address)
            
            self._logger.info(f"<Tool>: {'Deregistered' if response else 'Failed to deregister'}"
                              f" from Gateway at [{self._gateway_address}]")

            # Cancel heartbeat task
            if self._heartbeat_task and not self._heartbeat_task.done():
                self._heartbeat_task.cancel()
                try:
                    await self._heartbeat_task
                except asyncio.CancelledError:
                    pass

            self._gateway_address = None

        except grpc.aio.AioRpcError as e:
            self._logger.error(f"<Tool>: Failed to deregister from Gateway at [{self._gateway_address}] - "
                               f"RPC Error: {e.code()}, details: {e.details()}")
            if e.code() == grpc.StatusCode.UNKNOWN:
                # Handle BrokenPipeError
                pass
            raise
        except Exception as e:
            self._logger.error(f"<Tool>: Failed to deregister from Gateway at [{self._gateway_address}] - "
                               f"Exception: {str(e)}")
            
    async def update_toolbox_info(self, new_toolbox_info: pb2.ToolBoxInfo) -> None:
        """Update the tool info with the gateway.
        Args:
            new_toolbox_info: New toolbox information to update
        """
        self.toolbox_info = new_toolbox_info
        if self._gateway_address:
            stub = self._connection_pool.get_stub(self._gateway_address)
            
            node_info = pb2.Peer()
            node_info.toolbox_info.CopyFrom(new_toolbox_info)
            
            response = await stub.UpdateNodeInfo(pb2.UpdateNodeInfoRequest(node_info=node_info))
            
            self._logger.info(f"<Tool>: {'Updated' if response.success else 'Failed to update'} "
                              f"toolbox info with Gateway at [{self._gateway_address}]")