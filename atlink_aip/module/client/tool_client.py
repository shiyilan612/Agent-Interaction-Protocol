# -*- coding: utf-8 -*-
"""
Created on Wed Apr 23 12:00:00 2025

@author: haixinwa
"""
import grpc
from ...grpc_service import ToolServiceStub
from ...grpc_service.type import ToolRequest, ToolResponse
from ...session import ToolClientSession


class ToolClient:
    def __init__(self, server_address, stub=ToolServiceStub, callable_func="CallTool"):
        self.channel = None
        self.session = None
        self.server_address = server_address  # 保存连接参数
        self.stub = stub
        self.callable_func = callable_func

    async def __aenter__(self):
        return await self.start()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        return False

    async def start(self):
        self.channel = grpc.aio.insecure_channel(self.server_address)
        await self.channel.channel_ready()
        unary_unary_call = getattr(self.stub(self.channel), self.callable_func)
        self.session = ToolClientSession(unary_unary_call)
        await self.session.activate()

        return self

    async def send_request(self, sender_id:str, receiver_id:str, tool_name:str, arguments:str) -> ToolResponse:
        if not self.session:
            raise RuntimeError("Not connected")
        request = ToolRequest(
            sender_id=sender_id,
            receiver_id=receiver_id,
            session_id=self.session.session_id,
            tool_name=tool_name,
            arguments=arguments
        )
        response = await self.session.send(request)

        return response

    async def close(self):
        """close the connection and session"""
        if self.session:
            await self.session.close()
        if self.channel:
            await self.channel.close()