# -*- coding: utf-8 -*-
"""
Created on Mon Apr 21 12:00:00 2025

@author: haixinwa
"""
import grpc
import asyncio
from typing import Callable

from ...grpc_service import AgentService
from ...grpc_service.type import AgentMessage, AgentInfo, SessionStatus
from ...session import AgentServerSessionManager


class AgentServer(AgentService):
    def __init__(self, agent_info: AgentInfo):
        super().__init__(agent_info.to_grpc())
        self.session_mgr = AgentServerSessionManager(self._logger)

    async def receive_request(self):
        return await self.session_mgr.get_request()

    async def send_response(self, session_id: str, message: AgentMessage):
        return await self.session_mgr.put_response(session_id, message)

    async def send_session_handler(self, session_id: str, handler: Callable):
        return await self.session_mgr.put_session_handler(session_id, handler)

    async def get_session_output(self, session_id: str):
        return await self.session_mgr.get_session_output(session_id)

    async def CallAgent(self, request_iterator, context):
        # Phase I: Receive the first package and verify the START QUEST
        try:
            _first_message = await request_iterator.__aiter__().__anext__()
            first_message = AgentMessage.from_grpc(_first_message)
            
            sender_id = first_message.sender_id
            receiver_id = first_message.receiver_id
            
            self._logger.debug(f"<Agent>: [AgentMessage {sender_id} -> {receiver_id}] "
                               f"Received request")

            if first_message.session_status != SessionStatus.START_QUEST:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("First message must be START_QUEST")
                self._logger.error(f"<Agent>: [AgentMessage {sender_id} -> {receiver_id}] "
                               f"First message is not START_QUEST")
                return

            client_session_id = first_message.session_id
            if not client_session_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Missing session_id in START_QUEST")
                self._logger.error(f"<Agent>: [AgentMessage {sender_id} -> {receiver_id}] "
                               f"Missing session_id in START_QUEST")
                return

            # initialize the server session using the client session id
            session = await self.session_mgr.create_or_get_session(session_id=client_session_id, client_id=sender_id)

        except StopAsyncIteration:
            context.set_code(grpc.StatusCode.ABORTED)
            context.set_details("Empty request stream")
            self._logger.error(f"<Agent>: [AgentMessage] Empty request stream")

            return

        # Phase II: Handle the flowed messages
        async def receive_requests():
            await self.session_mgr.put_request(first_message)
            async for request in request_iterator:
                self._logger.debug(f"<Agent>: [AgentMessage {sender_id} -> {receiver_id}] "
                               f"Received request")
                await self.session_mgr.put_request(AgentMessage.from_grpc(request))

        receive_task = asyncio.create_task(receive_requests())

        try:
            async for response in session.get_response():
                self._logger.debug(f"<Agent>: [AgentMessage {receiver_id} -> {sender_id}] "
                               f"Send response")
                yield response.to_grpc()
        except RuntimeError as e:
            self._logger.error(f"<Agent> Failed to get response for client Agnet [{sender_id}]"
                                f" in session [{session.session_id}]"
                                f". INTERNAL Error: {e.code()}, details: {e.details()}")
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details(f"{str(e)}")
            
            return
        finally:
            receive_task.cancel()
            await self.session_mgr.close_session(client_session_id)
            self._logger.debug(f"<Agent>: Session [{session.session_id}] closed")