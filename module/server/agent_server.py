# -*- coding: utf-8 -*-
"""
Created on Mon Apr 21 12:00:00 2025

@author: haixinwa & xmkang
"""
import grpc
import asyncio
from typing import Callable

from grpc_service import AgentService
from grpc_service.type import AgentMessage, AgentInfo, SessionStatus
from session import AgentServerSessionManager


class AgentServer(AgentService):
    def __init__(self, agent_info: AgentInfo, process_request_func: Callable):
        super().__init__(agent_info.to_grpc())
        self.session_mgr = AgentServerSessionManager()
        self.process_request_func = process_request_func

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
            session = await self.session_mgr.create_or_get_session(client_session_id, self.process_request_func)

        except StopAsyncIteration:
            context.set_code(grpc.StatusCode.ABORTED)
            context.set_details("Empty request stream")

            return

        # Phase II: Handle the flowed message flow
        async def receive_requests():
            await session.put_request(first_message)
            async for request in request_iterator:
                await session.put_request(AgentMessage.from_grpc(request))

        receive_task = asyncio.create_task(receive_requests())

        try:
            async for response in session.get_response():
                yield response.to_grpc()
        finally:
            receive_task.cancel()
            await self.session_mgr.close_session(client_session_id)
            
            
    async def subscribe_to_nodes(self, node_ids=list()):
        """Subscribe to updates from specific nodes or all nodes"""
        return await super().subscribe_to_nodes(node_ids)
    
    async def unsubscribe_from_nodes(self, node_ids=list()):
        """Unsubscribe from updates from specific nodes or all nodes"""
        return await super().unsubscribe_from_nodes(node_ids)
    
    async def update_agent_info(self, new_agent_info):
        """Update the agent info with the gateway"""
        # Store the new info in the underlying service
        self.agent_info = new_agent_info.to_grpc()
        # The AgentService._check_agent_info_updates will detect the change
        # and notify the gateway on the next update interval