import grpc
import asyncio
from typing import Dict, Union, AsyncIterable
from .utils import ConnectionPool
from .schema_pb2_grpc import AgentServiceServicer, GatewayServiceStub, add_AgentServiceServicer_to_server
from . import schema_pb2


class AgentService(AgentServiceServicer):
    def __init__(self, agent_id: str):
        self.agent_id = agent_id
        self.server = None
        self.address = ""

        self.peers: Dict[str, Union[schema_pb2.AgentInfo, schema_pb2.ToolInfo]] = {}
        self.connection_pool = ConnectionPool()

    async def handle_outgoing_message(self) -> schema_pb2.AgentMessage:
        """子类需要实现消息发送逻辑"""
        raise NotImplementedError

    async def handle_incoming_message(self, message: schema_pb2.AgentMessage):
        """子类需要实现消息接收逻辑"""
        raise NotImplementedError

    async def process_agent_message(self, message: schema_pb2.AgentMessage) -> schema_pb2.AgentMessage:
        """子类需要实现CallAgent消息处理逻辑"""
        raise NotImplementedError

    async def _send_messages(self, stream):
        while True:
            message = await self.handle_outgoing_message()
            await stream.write(message)

    async def _receive_messages(self, stream):
        async for response in stream:
            await self.handle_incoming_message(response)

    async def _update_peers(self, new_peers):
        for peer in new_peers:
            set_field = peer.WhichOneof("info_type")
            if set_field == "agent_info":
                agent_info = peer.agent_info
                self.peers.update({agent_info.agent_id: agent_info})
            elif set_field == "tool_info":
                tool_info = peer.tool_info
                self.peers.update({tool_info.tool_id: tool_info})
            else:
                raise ValueError

    async def CallAgent(self,
                        request_iterator: AsyncIterable[schema_pb2.AgentMessage],
                        context:grpc.aio.ServicerContext) -> AsyncIterable[schema_pb2.AgentMessage]:
        async for message in request_iterator:
            # 异步处理消息
            processed_msg = await self.process_agent_message(message)
            # 立即返回响应
            yield processed_msg

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

    async def connect_to_gateway(self, gateway_addr: str):
        await self.connection_pool.create_stub(gateway_addr, GatewayServiceStub)
        stub = self.connection_pool.get_stub(gateway_addr)

        try:
            response = await stub.RegisterAgent(schema_pb2.AgentInfo(
                agent_id=self.agent_id,
                address=self.address))   # register agent

            await self._update_peers(response.peers) # update peers

            print(f"<{self.agent_id}>: RegisterResponse from GW ({gateway_addr})")

        except grpc.aio.AioRpcError as e:
            print(f"RPC Error: {e.details()}")
            if e.code() == grpc.StatusCode.UNKNOWN:
                # 处理 BrokenPipeError
                pass
        except Exception as e:
            print(f"其他异常: {str(e)}")

    async def create_agent_route_stream(self, gateway_addr: str):
        stub = self.connection_pool.get_stub(gateway_addr)
        stream = stub.RouteAgentCalling()
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

    async def get_gateway_node(self, gateway_addr: str):
        stub = self.connection_pool.get_stub(gateway_addr)

        try:
            # 注册Agent
            response = await stub.GetNodes(schema_pb2.GetNodesRequest(agent_id=self.agent_id))

            # load peers
            await self._update_peers(response.peers)  # update peers

            print(f"<{self.agent_id}>: Update peers from GW ({gateway_addr})")

        except grpc.aio.AioRpcError as e:
            print(f"RPC Error: {e.details()}")
            if e.code() == grpc.StatusCode.UNKNOWN:
                # 处理 BrokenPipeError
                pass
        except Exception as e:
            print(f"其他异常: {str(e)}")
