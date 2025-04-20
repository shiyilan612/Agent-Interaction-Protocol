# -*- coding: utf-8 -*-
"""
Created on Fri Apr 16 15:00:00 2025

@author: haixinwa
"""

# -*- coding: utf-8 -*-

import grpc
import uuid
from typing import Dict, Union, AsyncIterable
from .utils import ConnectionPool

# Import the generated proto modules
from .schema_pb2 import AgentInfo, RegisterAgentResponse, ToolInfo, RegisterToolResponse, \
    AgentMessage, ToolRequest, ToolResponse, GetNodesRequest, GetNodesResponse, \
    Peer
from .schema_pb2_grpc import GatewayServiceServicer, add_GatewayServiceServicer_to_server
from .schema_pb2_grpc import AgentServiceStub, ToolServiceStub


class GatewayService(GatewayServiceServicer):
    def __init__(self, 
                 address: str,
                 gw_id: str = None):
        self.address = address
        self.gw_id = gw_id if gw_id else f"gateway_{str(uuid.uuid4())}"

        # init registry dict
        self._registry: Dict[str, Union[AgentInfo, ToolInfo]] = {}

        # gRPC server for this tool service
        self._server = None

        #Stubs of nodes connected to this tool servic
        self._connection_pool = ConnectionPool()

    async def _forward_agent_message(self, message: AgentMessage) -> AsyncIterable[AgentMessage]:
        """消息转发核心逻辑"""
        receiver_info = self._registry.get(message.receiver_id)
        if not receiver_info:
            print(f"<GW>: Routing failed: Receiver {message.receiver_id} not found")
            return

        stub = self._connection_pool.get_stub(receiver_info.address)

        try:
            # 调用流方法
            stream = stub.CallAgent()

            # 发送原始消息
            await stream.write(message)

            # 处理响应流
            async for response in stream:
                yield response

        except grpc.RpcError as e:
            print(f"<GW>: Forwarding to {receiver_info.address} failed: {e.code()}")
            del self._registry[message.receiver_id] # 移除失效节点
            return

    async def _forward_tool_request(self, request: ToolRequest) -> ToolResponse:
        """消息转发核心逻辑"""
        receiver_info = self._registry.get(request.receiver_id)
        if not receiver_info:
            print(f"<GW>: Routing failed: Receiver {request.receiver_id} not found")
            return

        stub = self._connection_pool.get_stub(receiver_info.address)

        try:
            response = await stub.CallTool(request)

            return response

        except grpc.RpcError as e:
            print(f"<GW>: Forwarding to {receiver_info.address} failed: {e.code()}")
            del self._registry[request.receiver_id] # 移除失效节点
            return

    async def _collect_node_peers(self) -> list:
        peers = list()
        for info in list(self._registry.values()):
            peer = Peer()
            peer.agent_info.CopyFrom(info)
            peers.append(peer)

        return peers

    async def RouteAgentCalling(self,
                                request_iterator: AsyncIterable[AgentMessage],
                                context: grpc.aio.ServicerContext) -> AsyncIterable[AgentMessage]:
        """消息路由主入口"""
        async for message in request_iterator:
            # route 响应流
            async for response in self._forward_agent_message(message):
                yield response

    async def RouteToolCalling(self,
                               request: ToolRequest,
                               context: grpc.aio.ServicerContext) -> ToolResponse:
        response = await self._forward_tool_request(request)
        return response

    async def RegisterAgent(self,
                            request: AgentInfo,
                            context: grpc.aio.ServicerContext) -> RegisterAgentResponse:

        self._registry[request.agent_id] = request
        await self._connection_pool.create_stub(request.address, AgentServiceStub)
        print(f"<GW>: Register {request.agent_id} (addr in {request.address})")

        peers = await self._collect_node_peers()  # collect peers

        return RegisterAgentResponse(
            success=True,
            peers=peers
        )

    async def RegisterTool(self,
                           request: ToolInfo,
                           context: grpc.aio.ServicerContext) -> RegisterToolResponse:
        self._registry[request.tool_id] = request
        await self._connection_pool.create_stub(request.address, ToolServiceStub)
        print(f"<GW>: Register {request.tool_id} (addr in {request.address})")
        return RegisterToolResponse(
            success=True
        )

    async def GetNodes(self,
                       request: GetNodesRequest,
                       context: grpc.aio.ServicerContext) -> GetNodesResponse:

        print(f"<GW>: Agent {request.agent_id} request nodes info")
        peers = await self._collect_node_peers()  # collect peers

        return GetNodesResponse(
            peers=peers
        )

    async def start(self):
        self._server = grpc.aio.server()
        add_GatewayServiceServicer_to_server(self, self._server)
        self._server.add_insecure_port(self.address)
        await self._server.start()
        print(f"<{self.gw_id}>: Gateway {self.gw_id} started on {self.address}")

        # Wait for termination
        try:
            await self._server.wait_for_termination()
        finally:
            #  Ensure proper server shutdown
            await self._server.stop(1)  # 1 second timeout

    async def stop(self) -> None:
        """Stop the Agent service gRPC server."""
        if self._server:
            await self._server.stop(grace=None)
            print(f"Agent service at {self.address} stopped")
        # Close all connections in the pool
        await self._connection_pool.close_all()