import grpc
from typing import Dict, Union, AsyncIterable
from .utils import ConnectionPool
from .schema_pb2_grpc import (GatewayServiceServicer,
                              AgentServiceStub, ToolServiceStub,
                              add_GatewayServiceServicer_to_server)
from . import schema_pb2


class GatewayService(GatewayServiceServicer):
    def __init__(self, gw_id: str):
        self.gw_id = gw_id
        self.server = None
        self.address = ""

        self.registry: Dict[str, Union[schema_pb2.AgentInfo, schema_pb2.ToolInfo]] = {}
        self.connection_pool = ConnectionPool()

    async def _forward_agent_message(self, message: schema_pb2.AgentMessage) -> AsyncIterable[schema_pb2.AgentMessage]:
        """消息转发核心逻辑"""
        receiver_info = self.registry.get(message.receiver_id)
        if not receiver_info:
            print(f"<GW>: Routing failed: Receiver {message.receiver_id} not found")
            return

        stub = self.connection_pool.get_stub(receiver_info.address)

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

    async def _forward_tool_request(self, request: schema_pb2.ToolRequest) -> schema_pb2.ToolResponse:
        """消息转发核心逻辑"""
        receiver_info = self.registry.get(request.receiver_id)
        if not receiver_info:
            print(f"<GW>: Routing failed: Receiver {request.receiver_id} not found")
            return

        stub = self.connection_pool.get_stub(receiver_info.address)

        try:
            response = await stub.CallTool(request)

            return response

        except grpc.RpcError as e:
            print(f"<GW>: Forwarding to {receiver_info.address} failed: {e.code()}")
            del self.registry[request.receiver_id] # 移除失效节点
            return

    async def RouteAgentCalling(self,
                                request_iterator: AsyncIterable[schema_pb2.AgentMessage],
                                context: grpc.aio.ServicerContext) -> AsyncIterable[schema_pb2.AgentMessage]:
        """消息路由主入口"""
        async for message in request_iterator:
            # route 响应流
            async for response in self._forward_agent_message(message):
                yield response

    async def RouteToolCalling(self,
                               request: schema_pb2.ToolRequest,
                               context: grpc.aio.ServicerContext) -> schema_pb2.ToolResponse:
        response = await self._forward_tool_request(request)
        return response

    async def RegisterAgent(self,
                            request: schema_pb2.AgentInfo,
                            context: grpc.aio.ServicerContext) -> schema_pb2.RegisterAgentResponse:

        self.registry[request.agent_id] = request
        await self.connection_pool.create_stub(request.address, AgentServiceStub)
        print(f"<GW>: Register {request.agent_id} (addr in {request.address})")

        # collect peers
        peers = list()
        for info in list(self.registry.values()):
            peer = schema_pb2.Peer()
            peer.agent_info.CopyFrom(info)
            peers.append(peer)

        return schema_pb2.RegisterAgentResponse(
            success=True,
            peers=peers
        )

    async def RegisterTool(self,
                           request: schema_pb2.ToolInfo,
                           context: grpc.aio.ServicerContext) -> schema_pb2.RegisterToolResponse:
        self.registry[request.tool_id] = request
        await self.connection_pool.create_stub(request.address, ToolServiceStub)
        print(f"<GW>: Register {request.tool_id} (addr in {request.address})")
        return schema_pb2.RegisterToolResponse(
            success=True
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