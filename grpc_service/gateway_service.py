# -*- coding: utf-8 -*-
"""
Created on Wed Apr 16 15:00:00 2025

@author: haixinwa & clleng & xmkang
"""

# -*- coding: utf-8 -*-

import grpc
import uuid
import asyncio
import time
from typing import Dict, Union, AsyncIterable, Set
from .utils import ConnectionPool
from logger import LoggerManager

# Import the generated proto modules
from .schema_pb2_grpc import GatewayServiceServicer, add_GatewayServiceServicer_to_server
from .schema_pb2_grpc import AgentServiceStub, ToolServiceStub, GatewayServiceStub
from . import schema_pb2 as pb2


class GatewayService(GatewayServiceServicer):
    def __init__(self, 
                 address: str,
                 gateway_id: str = None,
                 heartbeat_timeout: int = 30):
        self.address = address
        self.gateway_id = gateway_id if gateway_id else f"gateway_{str(uuid.uuid4())}"

        # init registry dict
        self._registry: Dict[str, Union[pb2.AgentInfo, pb2.ToolInfo]] = {}
        
        self._registered_addresses: Set[str] = set()

        # gRPC server for this tool service
        self._server = None
        
        self._logger_mgr = LoggerManager()
        self._logger = self._logger_mgr.get_logger(self.gateway_id)

        # stubs of nodes connected to this tool service
        self._connection_pool = ConnectionPool(self._logger)

        # Heartbeat monitoring
        self._heartbeat_monitor_task = None
        self._last_heartbeats: Dict[str, float] = {}  # Last heartbeat timestamps
        self.heartbeat_timeout = heartbeat_timeout  # heartbeat timeout in seconds
        self.heartbeat_interval = heartbeat_timeout // 3  # interval time for heartbeat check
        
    async def _forward_agent_message(self, message: pb2.AgentMessage) -> AsyncIterable[pb2.AgentMessage]:
        """Route agent message to the receiver.

        Args:
            message (pb2.AgentMessage): The message to be routed.
        Returns:
            AsyncIterable[pb2.AgentMessage]: The response from the receiver.
        """
        pass

    async def _forward_tool_request(self, request: pb2.ToolRequest) -> pb2.ToolResponse:
        """Route tool request to the receiver.

        Args:
            request (pb2.ToolRequest): The tool request to be routed.
        Returns:
            pb2.ToolResponse: The response from the receiver.
        """
        pass

    async def _collect_node_peers(self, domain: str='default') -> list:
        """
        Collects the node peers of specified domain in the registry.
        By default, it collects all peers in the registry.

        Args:
            domain (str): The domain to filter the peers. Default is 'default'.
        Returns:
            list: A list of Peer objects containing the node information.
        """
        peers = list()
        for info in list(self._registry.values()):
            if domain != 'default' and info.domain != domain:
                continue
            peer = pb2.Peer()
            if isinstance(info, pb2.AgentInfo):
                peer.agent_info.CopyFrom(info)
            elif isinstance(info, pb2.ToolInfo):
                peer.tool_info.CopyFrom(info)
            else:
                raise ValueError(f"Unknown node type: {type(info)}")
            peers.append(peer)

        return peers
    
    async def get_node_info(self, node_id: str) -> Union[pb2.AgentInfo, pb2.ToolInfo, None]:
        """ Retrieves the node info by its node id.

        Args:
            node_id (str): node id

        Returns:
            node_info (Union[pb2.AgentInfo, pb2.AgentInfo, None]): 
            node info if the node ID is found; otherwise, None
        """
        node_info = self._registry.get(node_id)
        if not node_info:
            return

        return node_info

    async def get_node_stub(self, node_id: str) -> Union[AgentServiceStub, ToolServiceStub, GatewayServiceStub, None]:
        """Check if the node is registered and return the stub.
        
        Args:
            node_id (str): node id

        Returns:
            stub (Union[AgentServiceStub, ToolServiceStub, GatewayServiceStub, None]): 
            stub if the node ID is found; otherwise, None
        """
        node_info = await self.get_node_info(node_id)
        if not node_info:
            return

        stub = self._connection_pool.get_stub(node_info.address)
        return stub
    
    async def _deregister_node(self, node_id: str) -> None:
        """Delete the node from the registry and close its connection.

        Args:
            node_id (str): node id
        """
        node_info = await self.get_node_info(node_id)
        if not node_info:
            self._logger.warning(f"<GW>: Try to remove node [{node_id}] but not found in registry.")
            return False
        else:
            del self._registry[node_id]
            del self._last_heartbeats[node_id]
            self._registered_addresses.discard(node_info.address)
            await self._connection_pool.close_stub(node_info.address)
            self._logger.info(f"<GW>: Remove node [{node_id}] from registry. Connection terminated.")
            return True

    async def _handle_server_termination(self):
        try:
            await self._server.wait_for_termination()
        except Exception as e:
            self._logger.error(f"<GW>: Error during gRPC server termination: {e}")
        finally:
            await self._server.stop(1)

    async def RouteAgentCalling(self,
                                request_iterator: AsyncIterable[pb2.AgentMessage],
                                context: grpc.aio.ServicerContext) -> AsyncIterable[pb2.AgentMessage]:
        """Route agent messages to the receiver and get the response.
        
        Args:
            request_iterator (AsyncIterable[pb2.AgentMessage]): The agent messages to be routed.
            context (grpc.aio.ServicerContext): The gRPC context.
            
        Returns:
            AsyncIterable[pb2.AgentMessage]: The response from the receiver.
        """
        pass

    async def RouteToolCalling(self,
                               request: pb2.ToolRequest,
                               context: grpc.aio.ServicerContext) -> pb2.ToolResponse:
        """Route tool request to the receiver.
        
        Args:
            request (pb2.ToolRequest): The tool request to be routed.
            context (grpc.aio.ServicerContext): The gRPC context.
            
        Returns:
            pb2.ToolResponse: The response from the receiver.
        """
        pass

    async def RegisterAgent(self,
                            request: pb2.AgentInfo,
                            context: grpc.aio.ServicerContext) -> pb2.RegisterAgentResponse:
        agent_id = request.agent_id
        address = request.address
        
        if agent_id in self._registry:
            self._logger.error(f"<GW>: Try to register an already registered Agent [{agent_id}].")
            context.set_code(grpc.StatusCode.ALREADY_EXISTS)
            context.set_details(f"Agent [{agent_id}] already registered.")
            
            return

        if address in self._registered_addresses:
            self._logger.error(f"<GW>: Agent [{agent_id}] Try to register an already "
                                f"registered address [{address}].")
            context.set_code(grpc.StatusCode.ALREADY_EXISTS)
            context.set_details(f"Address [{address}] already registered.")
            
            return
        
        self._registry[agent_id] = request
        self._registered_addresses.add(address)

        # Initialize heartbeat timestamp
        self._last_heartbeats[agent_id] = time.time()

        await self._connection_pool.create_stub(address, AgentServiceStub)
        self._logger.info(f"<GW>: Register Agent [{agent_id}], addr in [{address}]")

        peers = await self._collect_node_peers()  # collect peers

        return pb2.RegisterAgentResponse(
            success=True,
            peers=peers
        )

    async def RegisterTool(self,
                           request: pb2.ToolInfo,
                           context: grpc.aio.ServicerContext) -> pb2.RegisterToolResponse:
        tool_id = request.tool_id
        address = request.address
        
        if tool_id in self._registry:
            self._logger.error(f"<GW>: Try to register an already registered Tool [tool_id].")
            context.set_code(grpc.StatusCode.ALREADY_EXISTS)
            context.set_details(f"Tool [{tool_id}] already registered.")
            
            return

        if address in self._registered_addresses:
            self._logger.error(f"<GW>: Tool [{tool_id}] Try to register an already "
                                f"registered address [{address}].")
            context.set_code(grpc.StatusCode.ALREADY_EXISTS)
            context.set_details(f"Address [{address}] already registered.")
            
            return
        
        self._registry[tool_id] = request
        self._registered_addresses.add(address)

        # Initialize heartbeat timestamp
        self._last_heartbeats[tool_id] = time.time()

        await self._connection_pool.create_stub(address, ToolServiceStub)
        self._logger.info(f"<GW>: Register Tool [{tool_id}], addr in [{address}]")
        
        return pb2.RegisterToolResponse(
            success=True
        )
        
    async def DeregisterNode(self, 
                             request: pb2.DeregisterNodeRequest, 
                             context: grpc.aio.ServicerContext)  -> pb2.DeregisterNodeResponse:
        node_id = request.node_id
        
        return pb2.DeregisterNodeResponse(
            success=await self._deregister_node(node_id)
        )

    async def GetNodes(self,
                       request: pb2.GetNodesRequest,
                       context: grpc.aio.ServicerContext) -> pb2.GetNodesResponse:

        self._logger.info(f"<GW>: Agent [{request.agent_id}] requests nodes info")
        peers = await self._collect_node_peers(request.domain)  # collect peers

        return pb2.GetNodesResponse(
            peers=peers
        )

    async def Heartbeat(self,
                        request: pb2.HeartbeatRequest,
                        context: grpc.aio.ServicerContext) -> pb2.HeartbeatResponse:
        """Handle heartbeat requests from nodes."""
        sender_id = request.sender_id
        if sender_id not in self._registry:
            return pb2.HeartbeatResponse(
                success=False,
                message=f"Node {sender_id} not registered"
            )

        # Update heartbeat timestamp
        self._last_heartbeats[sender_id] = time.time()

        return pb2.HeartbeatResponse(
            success=True,
            message="Heartbeat received"
        )

    async def _monitor_heartbeats(self):
        """Monitor heartbeats and disconnect timed-out nodes."""
        while True:
            try:
                current_time = time.time()
                nodes_to_disconnect = []

                # Check all registered nodes
                for node_id, last_heartbeat in list(self._last_heartbeats.items()):
                    if (current_time - last_heartbeat) > self.heartbeat_timeout:
                        self._logger.debug(f"<GW>: Node {node_id} heartbeat timeout")
                        nodes_to_disconnect.append(node_id)

                # Disconnect timed-out nodes
                for node_id in nodes_to_disconnect:
                    await self._deregister_node(node_id)

                # sleep for a while before next check
                await asyncio.sleep(self.heartbeat_interval)

            except Exception as e:
                self._logger.error(f"Error in heartbeat monitor: {e}")
                await asyncio.sleep(5)

    async def start(self):
        self._server = grpc.aio.server()
        add_GatewayServiceServicer_to_server(self, self._server)
        self._server.add_insecure_port(self.address)
        await self._server.start()
        self._logger.info(f"<GW>: Gateway [{self.gateway_id}] started on [{self.address}]")
        asyncio.create_task(self._handle_server_termination())

        # Start heartbeat monitor
        self._heartbeat_monitor_task = asyncio.create_task(self._monitor_heartbeats())

        return self

    async def stop(self) -> None:
        """Stop the Gateway service gRPC server."""

        # Cancel heartbeat monitor
        if self._heartbeat_monitor_task and not self._heartbeat_monitor_task.done():
            self._heartbeat_monitor_task.cancel()
            try:
                await self._heartbeat_monitor_task
            except asyncio.CancelledError:
                pass

        if self._server:
            await self._server.stop(grace=10.0)
            self._logger.info(f"<GW>: Gateway service [{self.gateway_id}] at "
                              f"[{self.address}] stopped")
        # Close all connections in the pool
        await self._connection_pool.close_all()