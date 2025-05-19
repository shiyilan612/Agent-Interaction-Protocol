# -*- coding: utf-8 -*-
"""
Created on Thur Mon Apr 24 19:00:00 2025

@author: clleng
"""
import time
import grpc
import asyncio
from typing import Dict, AsyncIterable

from ...grpc_service import GatewayService
from ...grpc_service.type import AgentInfo, ToolInfo, AgentMessage, SessionStatus
from ...grpc_service import schema_pb2 as pb2
from ...session import GatewaySessionMagager, GatewaySession

class GatewayHost(GatewayService):
    def __init__(self,
                 address: str,
                 gateway_id: str = None):
        super().__init__(address=address, gateway_id=gateway_id)
        self.session_mgr = GatewaySessionMagager(logger=self._logger)
        
    async def RouteAgentCalling(self,
                                request_iterator: AsyncIterable[pb2.AgentMessage],
                                context: grpc.aio.ServicerContext) -> AsyncIterable[pb2.AgentMessage]:
        """Route agent messages to the receiver and get the response.
        
        Args:
            request_iterator (AsyncIterable[pb2.AgentMessage]): The agent messages to be routed.
            context (grpc.aio.ServicerContext): The gRPC context.
            
        Returns:
            AsyncIterable[pb2.AgentMessage]: The response from the receiver."""
            
        try:
            _first_message = await request_iterator.__aiter__().__anext__()
            first_message = AgentMessage.from_grpc(_first_message)
            
            sender_id = first_message.sender_id
            receiver_id = first_message.receiver_id

            # Update heartbeat timestamp for the sender
            if sender_id and sender_id in self._registry:
                self._last_heartbeats[sender_id] = time.time()
            
            if first_message.session_status != SessionStatus.START_QUEST:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("First message must be START_QUEST")
                self._logger.error(f"<GW>: [AgentMessage {sender_id} -> {receiver_id}] First message "
                                f"is not START_QUEST")
                return

            client_session_id = first_message.session_id
            if not client_session_id:
                context.set_code(grpc.StatusCode.INVALID_ARGUMENT)
                context.set_details("Missing session_id in START_QUEST")
                self._logger.error(f"<GW>: [AgentMessage {sender_id} -> {receiver_id}] Missing "
                                f"session_id in START_QUEST")
                return
            
            stub = await super().get_node_stub(first_message.receiver_id)
            if not stub:
                context.set_code(grpc.StatusCode.NOT_FOUND)
                context.set_details("Receiver not found")
                self._logger.error(f"<GW>: [AgentMessage {sender_id} -> {receiver_id}] Routing "
                                f"failed. Receiver not found")
                return
            
            session = await self.session_mgr.create_or_get_session(session_id=first_message.session_id,
                                                                sender_id=first_message.sender_id,
                                                                receiver_id=first_message.receiver_id,
                                                                stub=stub, 
                                                                callable_func="CallAgent")
        except StopAsyncIteration:
            context.set_code(grpc.StatusCode.ABORTED)
            context.set_details("Empty request stream")
            self._logger.error(f"<GW>: [AgentMessage] Empty request stream")
            
            return
        
        await self._forward_agent_message(session, _first_message)
        async def forward_message():
            async for message in request_iterator:
                await self._forward_agent_message(session, message)
                
        forward_task = asyncio.create_task(forward_message())
                
        try:
            # yield the responses from the session
            async for response in session.get_response():
                self._logger.info(f"<GW>: [AgentMessage {receiver_id} -> {sender_id}] Route "
                                  f"response in session [{session.session_id}]")
                yield response.to_grpc()
                
                if response.session_status == SessionStatus.STOP_RESPONSE:
                    # close the session if the response is STOP_RESPONSE
                    break
        except grpc.RpcError as e:
            self._logger.error(f"<GW> [AgentMessage {receiver_id} -> {sender_id}] Error happened"
                                f" in session [{session.session_id}]. "
                                f"RPC Error: {e.code()}, details: {e.details()}")
            context.set_code(e.code())
            context.set_details(e.details())
            # delete failed node
            await super()._deregister_node(session.receiver_id)
            return
        finally:
            forward_task.cancel()
            await self.session_mgr.close_session(session_id=session.session_id)
            self._logger.info(f"<GW>: Session [{session.session_id}] closed")
            
        
    async def _forward_agent_message(self, session: GatewaySession, message: pb2.AgentMessage) -> None:
        """Route agent message to the receiver.

        Args:
            message (pb2.AgentMessage): The message to be routed.
        Returns:
            AsyncIterable[pb2.AgentMessage]: The response from the receiver.
        """
        # route the message to the receiver by session
        await session.enqueue_forward_message(message)
        self._logger.info(f"<GW>: [AgentMessage {message.receiver_id} -> {message.sender_id}]"
                            f" Route request in session [{session.session_id}]")

            
    async def RouteToolCalling(self,
                               request: pb2.ToolRequest,
                               context: grpc.aio.ServicerContext) -> pb2.ToolResponse:
        """Route tool request to the receiver.
        
        Args:
            request (pb2.ToolRequest): The tool request to be routed.
            context (grpc.aio.ServicerContext): The gRPC context.
            
        Returns:
            pb2.ToolResponse: The response from the receiver.
        """
        sender_id = request.sender_id
        receiver_id = request.receiver_id

        # Update heartbeat timestamp for the sender
        if sender_id and sender_id in self._registry:
            self._last_heartbeats[sender_id] = time.time()
        
        stub = await super().get_node_stub(request.receiver_id)
        if not stub:
            context.set_code(grpc.StatusCode.NOT_FOUND)
            context.set_details("Receiver not found")
            self._logger.error(f"<GW>: [ToolRequest {sender_id} -> {receiver_id}]"
                               f" Routing failed. Receiver not found")
            return
        
        try:
            self._logger.info(f"<GW>: [ToolRequest {sender_id} -> {receiver_id}] Route request")
            response = await stub.CallTool(request)

            self._logger.info(f"<GW>: [ToolResponse {receiver_id} -> {sender_id}] Route response")
            return response

        except grpc.RpcError as e:
            self._logger.error(f"<GW>: [ToolResponse {sender_id} -> {receiver_id}] Route "
                  f"ToolCalling failed: RPC Error: {e.code()}, details: {e.details()}")
            # delete failed node
            await super()._deregister_node(request.receiver_id)
            return

    async def get_agents_info(self) -> Dict[str, AgentInfo]:
        """
        Get all registered agents.

        Returns:
            Dict[str, AgentInfo]: Dictionary of agent_id -> AgentInfo
        """
        agent_dict = {node_id: AgentInfo.from_grpc(info)
                      for node_id, info in self._registry.items() if isinstance(info, pb2.AgentInfo)}
        return agent_dict
    
    async def get_tools_info(self) -> Dict[str, ToolInfo]:
        """
        Get all registered tools.

        Returns:
            Dict[str, AgentInfo]: Dictionary of tool_id -> ToolInfo
        """
        tool_dict = {node_id: ToolInfo.from_grpc(info)
                     for node_id, info in self._registry.items() if isinstance(info, pb2.ToolInfo)}
        return tool_dict