import grpc
from typing import Callable
from grpc_service.type import ToolRequest, ToolResponse
from session import ToolClientSession


class ToolClient:
    def __init__(self, process_response_func: Callable):
        self.channel = None
        self.session = None
        self.process_response_func = process_response_func

    async def start(self, server_address, stub, callable_func="CallTool"):
        self.channel = grpc.aio.insecure_channel(server_address)
        await self.channel.channel_ready()
        unary_unary_call = getattr(stub(self.channel), callable_func)
        self.session = ToolClientSession(unary_unary_call)
        await self.session.activate()

        return self

    async def send_request(self, request: ToolRequest) -> ToolResponse:
        if not self.session:
            raise RuntimeError("Not connected")
        request.session_id = self.session.session_id
        response = await self.session.send(request)

        return response

    async def close(self):
        """close the connection and session"""
        if self.session:
            await self.session.close()
        if self.channel:
            await self.channel.close()