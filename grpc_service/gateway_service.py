# -*- coding: utf-8 -*-
"""
Created on Wed Apr 16 15:00:00 2025

@author: haixinwa & clleng
"""

# -*- coding: utf-8 -*-

import grpc
import uuid
import asyncio
from typing import Dict, Union, AsyncIterable, Set
from .utils import ConnectionPool

# Import the generated proto modules
from .schema_pb2_grpc import GatewayServiceServicer, add_GatewayServiceServicer_to_server
from .schema_pb2_grpc import AgentServiceStub, ToolServiceStub, GatewayServiceStub
from . import schema_pb2 as pb2


class GatewayService(GatewayServiceServicer):
    def __init__(self, 
                 address: str,
                 gateway_id: str = None):
        self.address = address
        self.gateway_id = gateway_id if gateway_id else f"gateway_{str(uuid.uuid4())}"

        # init registry dict
        self._registry: Dict[str, Union[pb2.AgentInfo, pb2.ToolInfo]] = {}
        
        self._registered_addresses: Set[str] = set()

        # gRPC server for this tool service
        self._server = None

        # stubs of nodes connected to this tool service
        self._connection_pool = ConnectionPool()

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
            node_info (Union[pb2.AgentInfo, pb2.AgentInfo, None]): node info if the node ID is found; otherwise, None
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
            stub (Union[AgentServiceStub, ToolServiceStub, GatewayServiceStub, None]): stub if the node ID is found; otherwise, None
        """
        node_info = await self.get_node_info(node_id)
        if not node_info:
            return

        stub = self._connection_pool.get_stub(node_info.address)
        return stub
    
    # TODO: 需要考虑节点的注销，proto里面需要定义注销信息
    async def deregister_node(self, node_id: str) -> None:
        """Delete the node from the registry and close its connection.

        Args:
            node_id (str): node id
        """
        node_info = await self.get_node_info(node_id)
        if not node_info:
            print(f"<GW>: Try to remove Node {node_id} but not found in registry.")
        else:
            del self._registry[node_id]
            self._registered_addresses.discard(node_info.address)
            await self._connection_pool.close_stub(node_info.address)
            print(f"<GW>: Node {node_id} removed from registry and connection closed.")

    async def _handle_server_termination(self):
        try:
            await self._server.wait_for_termination()
        except Exception as e:
            print(f"Error during server termination: {e}")
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
            AsyncIterable[pb2.AgentMessage]: The response from the receiver."""
        pass

    async def RouteToolCalling(self,
                               request: pb2.ToolRequest,
                               context: grpc.aio.ServicerContext) -> pb2.ToolResponse:
        response = await self._forward_tool_request(request)
        return response

    async def RegisterAgent(self,
                            request: pb2.AgentInfo,
                            context: grpc.aio.ServicerContext) -> pb2.RegisterAgentResponse:
        if request.agent_id in self._registry:
            print(f"<GW>: Agent {request.agent_id} already registered.")
            context.set_code(grpc.StatusCode.ALREADY_EXISTS)
            context.set_details(f"Agent {request.agent_id} already registered.")
            
            return

        if request.address in self._registered_addresses:
            print(f"<GW>: Address {request.address} already registered.")
            context.set_code(grpc.StatusCode.ALREADY_EXISTS)
            context.set_details(f"Address {request.address} already registered.")
            
            return
        
        self._registry[request.agent_id] = request
        self._registered_addresses.add(request.address)
        await self._connection_pool.create_stub(request.address, AgentServiceStub)
        print(f"<GW>: Register {request.agent_id} (addr in {request.address})")

        peers = await self._collect_node_peers()  # collect peers

        return pb2.RegisterAgentResponse(
            success=True,
            peers=peers
        )

    async def RegisterTool(self,
                           request: pb2.ToolInfo,
                           context: grpc.aio.ServicerContext) -> pb2.RegisterToolResponse:
        if request.tool_id in self._registry:
            print(f"<GW>: Tool {request.tool_id} already registered.")
            context.set_code(grpc.StatusCode.ALREADY_EXISTS)
            context.set_details(f"Tool {request.tool_id} already registered.")
            
            return

        if request.address in self._registered_addresses:
            print(f"<GW>: Address {request.address} already registered.")
            context.set_code(grpc.StatusCode.ALREADY_EXISTS)
            context.set_details(f"Address {request.address} already registered.")
            
            return
        
        self._registry[request.tool_id] = request
        self._registered_addresses.add(request.address)
        await self._connection_pool.create_stub(request.address, ToolServiceStub)
        print(f"<GW>: Register {request.tool_id} (addr in {request.address})")
        
        return pb2.RegisterToolResponse(
            success=True
        )

    async def GetNodes(self,
                       request: pb2.GetNodesRequest,
                       context: grpc.aio.ServicerContext) -> pb2.GetNodesResponse:

        print(f"<GW>: Agent {request.agent_id} request nodes info")
        peers = await self._collect_node_peers(request.domain)  # collect peers

        return pb2.GetNodesResponse(
            peers=peers
        )

    async def start(self):
        self._server = grpc.aio.server()
        add_GatewayServiceServicer_to_server(self, self._server)
        self._server.add_insecure_port(self.address)
        await self._server.start()
        print(f"<{self.gateway_id}>: Gateway {self.gateway_id} started on {self.address}")
        asyncio.create_task(self._handle_server_termination())

        return self

    async def stop(self) -> None:
        """Stop the Gateway service gRPC server."""
        if self._server:
            await self._server.stop(grace=None)
            print(f"Gateway service at {self.address} stopped")
        # Close all connections in the pool
        await self._connection_pool.close_all()