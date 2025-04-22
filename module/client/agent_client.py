import grpc
import asyncio
from typing import Dict, Callable
from grpc_service.type import AgentMessage
from session import AgentClientSession


class AgentClient:
    def __init__(self, process_response_func: Callable):
        self.channel = None
        self.session = None
        self._response_task = None
        self.process_response_func = process_response_func

    async def _process_responses(self):
        async for response in self.session.stream_responses():
            await self.process_response_func(response)

    async def start(self, server_address, stub, callable_func="CallAgent"):
        self.channel = grpc.aio.insecure_channel(server_address)
        await self.channel.channel_ready()
        stream_stream_call = getattr(stub(self.channel), callable_func)()
        self.session = AgentClientSession(stream_stream_call)
        await self.session.create_session()

        if not self.session:
            raise RuntimeError("Not connected")

        self._response_task = asyncio.create_task(self._process_responses())
        return self

    async def send_message(self, message: AgentMessage):
        if not self.session:
            raise RuntimeError("Not connected")
        message.session_id = self.session.session_id
        await self.session.send(message)

    async def wait_completion(self):
        """Wait for the session to finish"""
        if not self.session:
            raise RuntimeError("Not connected")
        return await self.session.wait_for_stop()

    async def close(self):
        """close the connection and session"""
        if self.session:
            await self.session.close()
        if self.channel:
            await self.channel.close()
        if self._response_task and not self._response_task.done():
            self._response_task.cancel()
            try:
                await self._response_task
            except asyncio.CancelledError:
                pass