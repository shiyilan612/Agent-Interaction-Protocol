import grpc
from .utils import ConnectionPool
from .schema_pb2_grpc import ToolServiceServicer, GatewayServiceStub, add_ToolServiceServicer_to_server
from . import schema_pb2


class ToolService(ToolServiceServicer):
    def __init__(self, tool_id: str):
        self.tool_id = tool_id
        self.server = None
        self.address = ""

        self.connection_pool = ConnectionPool()

    async def process_tool_request(self, message: schema_pb2.ToolRequest) -> schema_pb2.ToolResponse:
        """子类需要实现CallTool消息处理逻辑"""
        raise NotImplementedError

    async def CallTool(self,
                        request: schema_pb2.ToolRequest,
                        context:grpc.aio.ServicerContext) -> schema_pb2.ToolResponse:
        processed_msg = await self.process_tool_request(request)
        return processed_msg

    async def start(self, port: int):
        self.server = grpc.aio.server()
        add_ToolServiceServicer_to_server(self, self.server)
        self.address = f'localhost:{port}'
        self.server.add_insecure_port(self.address)
        await self.server.start()
        print(f"<{self.tool_id}>: Tool {self.tool_id} started on {self.address}")

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
            # 注册Tool
            response = await stub.RegisterTool(schema_pb2.ToolInfo(
                tool_id=self.tool_id,
                address=self.address))

            print(f"<{self.tool_id}>: RegisterResponse from GW ({gateway_addr})")

        except grpc.aio.AioRpcError as e:
            print(f"RPC Error: {e.details()}")
            if e.code() == grpc.StatusCode.UNKNOWN:
                # 处理 BrokenPipeError
                pass
        except Exception as e:
            print(f"其他异常: {str(e)}")
