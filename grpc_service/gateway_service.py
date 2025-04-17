import grpc
from typing import Dict, AsyncIterable
from .utils import ConnectionPool
from .schema_pb2_grpc import GatewayServiceServicer, AgentServiceStub, add_GatewayServiceServicer_to_server
from . import schema_pb2


class GatewayService(GatewayServiceServicer):
    def __init__(self, gw_id: str):
        self.gw_id = gw_id
        self.server = None
        self.address = ""

        self.registry: Dict[str, schema_pb2.RouteInfo] = {}
        self.connection_pool = ConnectionPool()

    async def _forward_message(self, message: schema_pb2.MultiModalMessage) -> AsyncIterable[schema_pb2.MultiModalMessage]:
        """消息转发核心逻辑"""
        receiver_info = self.registry.get(message.receiver_id)
        if not receiver_info:
            print(f"<GW>: Routing failed: Receiver {message.receiver_id} not found")
            return

        stub = self.connection_pool.get_stub(receiver_info.address)

        try:
            # 调用流方法
            stream = stub.StreamCommunicate()

            # 发送原始消息
            await stream.write(message)

            # 处理响应流
            async for response in stream:
                yield response

        except grpc.RpcError as e:
            print(f"<GW>: Forwarding to {receiver_info.address} failed: {e.code()}")
            del self.registry[message.receiver_id] # 移除失效节点
            return

    async def RouteMessage(self, request_iterator: AsyncIterable[schema_pb2.MultiModalMessage],
                           context) -> AsyncIterable[schema_pb2.MultiModalMessage]:
        """消息路由主入口"""
        async for message in request_iterator:
            # route 响应流
            async for response in self._forward_message(message):
                yield response

    async def RegisterAgent(self, request: schema_pb2.RouteInfo, context) -> schema_pb2.RegisterResponse:
        self.registry[request.agent_id] = request
        await self.connection_pool.create_stub(request.address, AgentServiceStub)
        print(f"<GW>: Register {request.agent_id} (addr in {request.address})")
        return schema_pb2.RegisterResponse(
            success=True,
            peers=list(self.registry.values())
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