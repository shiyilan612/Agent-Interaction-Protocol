# -*- coding: utf-8 -*-
"""
Created on Fri Apr 16 15:00:00 2025

@author: haixinwa
"""

# -*- coding: utf-8 -*-

import grpc
from typing import Dict, Union, AsyncIterable
from .utils import ConnectionPool

# Import the generated proto modules
from .schema_pb2 import AgentInfo, RegisterAgentResponse, ToolInfo, RegisterToolResponse, \
    AgentMessage, ToolRequest, ToolResponse, GetNodesRequest, GetNodesResponse, \
    Peer
from .schema_pb2_grpc import GatewayServiceServicer, add_GatewayServiceServicer_to_server
from .schema_pb2_grpc import AgentServiceStub, ToolServiceStub


class GatewayService(GatewayServiceServicer):
    def __init__(self, gw_id: str):
        self.gw_id = gw_id
        self.server = None
        self.address = ""

        self.registry: Dict[str, Union[AgentInfo, ToolInfo]] = {}
        self._connection_pool = ConnectionPool()

    async def _forward_agent_message(self, message: AgentMessage) -> AsyncIterable[AgentMessage]:
        """消息转发核心逻辑"""
        receiver_info = self.registry.get(message.receiver_id)
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
            del self.registry[message.receiver_id] # 移除失效节点
            return

    async def _forward_tool_request(self, request: ToolRequest) -> ToolResponse:
        """消息转发核心逻辑"""
        receiver_info = self.registry.get(request.receiver_id)
        if not receiver_info:
            print(f"<GW>: Routing failed: Receiver {request.receiver_id} not found")
            return

        stub = self._connection_pool.get_stub(receiver_info.address)

        try:
            response = await stub.CallTool(request)

            return response

        except grpc.RpcError as e:
            print(f"<GW>: Forwarding to {receiver_info.address} failed: {e.code()}")
            del self.registry[request.receiver_id] # 移除失效节点
            return

    async def _collect_node_peers(self) -> list:
        peers = list()
        for info in list(self.registry.values()):
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

        self.registry[request.agent_id] = request
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
        self.registry[request.tool_id] = request
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

    async def start(self, port: int):
        self.server = grpc.aio.server()
        add_GatewayServiceServicer_to_server(self, self.server)
        self.address = f'localhost:{port}'
        self.server.add_insecure_port(self.address)
        await self.server.start()
        print(f"<{self.gw_id}>: Gateway {self.gw_id} started on {self.address}")

        # 等待结束
        try:
            await self.server.wait_for_termination()
        finally:
            # 确保正确关闭服务
            await self.server.stop(1)  # 1秒超时