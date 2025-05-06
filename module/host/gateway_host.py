# -*- coding: utf-8 -*-
"""
Created on Thur Mon Apr 24 19:00:00 2025

@author: clleng
"""
import grpc
import asyncio
from typing import Dict, AsyncIterable

from grpc_service import GatewayService
from grpc_service.type import AgentInfo, ToolInfo, AgentMessage, SessionStatus
from grpc_service import schema_pb2 as pb2
from session import GatewaySessionMagager, GatewaySession

class GatewayHost(GatewayService):
    def __init__(self,
                 address: str,
                 gateway_id: str = None):
        super().__init__(address=address, gateway_id=gateway_id)
        self.session_mgr = GatewaySessionMagager()
        
    async def RouteAgentCalling(self,
                                request_iterator: AsyncIterable[pb2.AgentMessage],
                                context: grpc.aio.ServicerContext) -> AsyncIterable[pb2.AgentMessage]:
        """Route agent messages to the receiver and get the response.
        
        Args:
            request_iterator (AsyncIterable[pb2.AgentMessage]): The agent messages to be routed.
            context (grpc.aio.ServicerContext): The gRPC context.
            
        Returns:
            AsyncIterable[pb2.AgentMessage]: The response from the receiver."""
            
        _first_message = await request_iterator.__aiter__().__anext__()
        first_message = AgentMessage.from_grpc(_first_message)
        
        stub = await super().get_node_stub(first_message.receiver_id)
        if not stub:
            print(f"<GW>: Routing failed: Receiver {first_message.receiver_id} not found")
            return
        
        session = await self.session_mgr.create_or_get_session(session_id=first_message.session_id,
                                                               sender_id=first_message.sender_id,
                                                               receiver_id=first_message.receiver_id,
                                                               stub=stub, 
                                                               callable_func="CallAgent")
        await self._forward_agent_message(session, _first_message)
        async def forward_message():
            async for message in request_iterator:
                await self._forward_agent_message(session, message)
                
        forward_task = asyncio.create_task(forward_message())
                
        try:
            # yield the responses from the session
            async for response in session.get_response():
                # print(f"Yielding response: {response.content[0]._text}")
                # response = AgentMessage.from_grpc(response)

                if response.session_status == SessionStatus.STOP_RESPONSE:
                    # close the session if the response is STOP_RESPONSE
                    forward_task.cancel()
                    await self.session_mgr.close_session(session_id=session.session_id)
                    print(f"<GW>: Session {session.session_id} closed")
                    yield response.to_grpc()
                    break

                yield response.to_grpc()
        except grpc.RpcError as e:
            receiver_info = await super().get_node_info(session.receiver_id)
            print(f"<GW>: Get Response from Agent {session.receiver_id} (address: {receiver_info.address}) failed: {e.code()}")
            # delete failed node
            await super().deregister_node(session.receiver_id)
            return
        finally:
            forward_task.cancel()
            await self.session_mgr.close_session(session_id=session.session_id)
        
    async def _forward_agent_message(self, session: GatewaySession, message: pb2.AgentMessage) -> None:
        """Route agent message to the receiver.

        Args:
            message (pb2.AgentMessage): The message to be routed.
        Returns:
            AsyncIterable[pb2.AgentMessage]: The response from the receiver.
        """
        message = AgentMessage.from_grpc(message)
        # print(f"Start to forward message: {message.content[0]._text}")

        try:
            # route the message to the receiver by session
            # print(f"Try to enqueue message {message.content[0]._text}")
            await session.enqueue_forward_message(message.to_grpc())

        except grpc.RpcError as e:
            receiver_info = await super().get_node_info(message.receiver_id)
            print(f"<GW>: Forwarding to Agent {message.receiver_id} (address: {receiver_info.address}) failed: {e.code()}")
            # delete failed node
            await super().deregister_node(message.receiver_id)

    async def _forward_tool_request(self, request: pb2.ToolRequest) -> pb2.ToolResponse:
        """Route tool request to the receiver.

        Args:
            request (pb2.ToolRequest): The tool request to be routed.
        Returns:
            pb2.ToolResponse: The response from the receiver.
        """
        stub = await super().get_node_stub(request.receiver_id)
        if not stub:
            print(f"<GW>: Routing failed: Receiver {request.receiver_id} not found")
            return

        try:
            response = await stub.CallTool(request)

            return response

        except grpc.RpcError as e:
            receiver_info = await super().get_node_info(request.receiver_id)
            print(f"<GW>: Forwarding to {receiver_info.address} failed: {e.code()}")
            # delete failed node
            await super().deregister_node(request.receiver_id)
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