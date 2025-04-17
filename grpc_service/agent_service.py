import grpc
import asyncio
from typing import Dict, Union, AsyncIterable, Optional
from .schema_pb2_grpc import (AgentServiceServicer, AgentServiceStub, GatewayServiceStub,
                              add_AgentServiceServicer_to_server)
from . import schema_pb2


class ConnectionPool:
    """gRPC连接池管理"""
    def __init__(self):
        self._channels: Dict[str, grpc.aio.Channel] = {}
        self._stubs: Dict[str, Union[AgentServiceStub, GatewayServiceStub]] = {}

    async def get_stub(self, address: str) -> Optional[Union[AgentServiceStub, GatewayServiceStub]]:
        """获取或创建指定地址的存根"""
        if address not in self._channels:
            try:
                channel = grpc.aio.insecure_channel(address)
                await channel.channel_ready()
                self._channels[address] = channel
                self._stubs[address] = AgentServiceStub(channel)
            except grpc.RpcError as e:
                print(f"Connection failed to {address}: {e.code()}")
                return None
        return self._stubs[address]

    async def close_all(self):
        """关闭所有连接"""
        closing_tasks = []
        for addr, channel in self._channels.items():
            closing_tasks.append(channel.close())
        await asyncio.gather(*closing_tasks, return_exceptions=True)
        self._channels.clear()
        self._stubs.clear()


class AgentService(AgentServiceServicer):
    def __init__(self, agent_id: str):
        self.agent_id = agent_id
        self.server = None
        self.address = ""
        self.gateway_addr = ""
        self.peers: Dict[str, schema_pb2.RouteInfo] = {}

    async def handle_outgoing_message(self) -> schema_pb2.MultiModalMessage:
        """子类需要实现RouteMessage消息发送逻辑"""
        raise NotImplementedError

    async def handle_incoming_message(self, message: schema_pb2.MultiModalMessage):
        """子类需要实现RouteMessage消息接收逻辑"""
        raise NotImplementedError

    async def process_comm_message(self, message: schema_pb2.MultiModalMessage) -> schema_pb2.MultiModalMessage:
        """子类需要实现StreamCommunicate消息处理逻辑"""
        raise NotImplementedError

    async def _send_messages(self, stream):
        while True:
            message = await self.handle_outgoing_message()
            await stream.write(message)

    async def _receive_messages(self, stream):
        async for response in stream:
            await self.handle_incoming_message(response)

    async def _handle_route_message_stream(self, stream):
        send_task = asyncio.create_task(self._send_messages(stream))
        recv_task = asyncio.create_task(self._receive_messages(stream))
        try:
            await asyncio.gather(send_task, recv_task)
        except grpc.aio.AioRpcError as e:
            print(f"RPC Error: {e.details()}")
            if e.code() == grpc.StatusCode.UNKNOWN:
                # 处理 BrokenPipeError
                pass
        except Exception as e:
            print(f"其他异常: {str(e)}")

    async def StreamCommunicate(self, request_iterator: AsyncIterable[schema_pb2.MultiModalMessage],
                                context) -> AsyncIterable[schema_pb2.MultiModalMessage]:
        async for message in request_iterator:
            # 异步处理消息
            processed_msg = await self.process_comm_message(message)
            # 立即返回响应
            yield processed_msg

    async def connect_to_gateway(self, gateway_addr: str):
        self.gateway_addr = gateway_addr
        async with grpc.aio.insecure_channel(self.gateway_addr) as channel:
            stub = GatewayServiceStub(channel)

            try:
                # 注册Agent
                response = await stub.RegisterAgent(schema_pb2.RouteInfo(
                    agent_id=self.agent_id,
                    address=self.address))
                self.peers = {peer.agent_id: peer for peer in response.peers}
                print(f"<{self.agent_id}>: RegisterResponse from GW ({gateway_addr})")
            except grpc.aio.AioRpcError as e:
                print(f"RPC Error: {e.details()}")
                if e.code() == grpc.StatusCode.UNKNOWN:
                    # 处理 BrokenPipeError
                    pass
            except Exception as e:
                print(f"其他异常: {str(e)}")

            stream = stub.RouteMessage()
            await self._handle_route_message_stream(stream)

    async def start(self, port: int):
        self.server = grpc.aio.server()
        add_AgentServiceServicer_to_server(self, self.server)
        self.address = f'localhost:{port}'
        self.server.add_insecure_port(self.address)
        await self.server.start()
        print(f"<{self.agent_id}>: Agent {self.agent_id} started on {self.address}")

        # 等待结束
        try:
            await self.server.wait_for_termination()
        finally:
            # 确保正确关闭服务
            await self.server.stop(1)  # 1秒超时
