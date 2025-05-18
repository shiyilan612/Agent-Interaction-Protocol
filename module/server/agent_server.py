# -*- coding: utf-8 -*-
"""
Created on Mon Apr 21 12:00:00 2025

@author: haixinwa
"""
import grpc
import asyncio
from typing import Callable

from grpc_service import AgentService
from grpc_service.type import AgentMessage, AgentInfo, SessionStatus
from session import AgentServerSessionManager


class AgentServer(AgentService):
    def __init__(self, agent_info: AgentInfo):
        super().__init__(agent_info.to_grpc())
        self.session_mgr = AgentServerSessionManager()

    async def CallAgent(self, request_iterator, context):
        # Phase I: Receive the first package and verify the START QUEST
        try:
            _first_message = await request_iterator.__aiter__().__anext__()
            first_message = AgentMessage.from_grpc(_first_message)

            if first_message.session_status != SessionStatus.START_QUEST:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("First message must be START_QUEST")
                return

            client_session_id = first_message.session_id
            if not client_session_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Missing session_id in START_QUEST")
                return

            # initialize the server session using the client session id
            session = await self.session_mgr.create_or_get_session(client_session_id)

        except StopAsyncIteration:
            context.set_code(grpc.StatusCode.ABORTED)
            context.set_details("Empty request stream")

            return

        # Phase II: Handle the flowed messages
        async def receive_requests():
            await session.put_request(first_message)
            async for request in request_iterator:
                await session.put_request(AgentMessage.from_grpc(request))

        receive_task = asyncio.create_task(receive_requests())

        try:
            async for response in session.get_response():
                yield response.to_grpc()
        except RuntimeError as e:
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"{str(e)}")
            
            return
        finally:
            receive_task.cancel()
            await self.session_mgr.close_session(client_session_id)

    async def get_request(self, session_id):
        session = await self.session_mgr.create_or_get_session(session_id)
        async for request in session.get_request():
            yield request

    async def send_response(self, session_id, message):
        session = await self.session_mgr.create_or_get_session(session_id)
        await session.put_response(message)

    def list_sessions(self):
        return list(self.session_mgr.active_sessions.keys())
