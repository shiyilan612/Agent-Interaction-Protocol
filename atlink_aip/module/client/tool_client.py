# -*- coding: utf-8 -*-
"""
Created on Wed Apr 23 12:00:00 2025

@author: haixinwa
"""
import grpc
from ...grpc_service.type import ToolRequest, ToolResponse
from ...session import ToolClientSession


class ToolClient:
    def __init__(self):
        self.channel = None
        self.session = None

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