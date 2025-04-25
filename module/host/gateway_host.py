# -*- coding: utf-8 -*-
"""
Created on Thur Mon Apr 24 19:00:00 2025

@author: clleng
"""
import grpc
import asyncio
from typing import Callable, Dict, Union, AsyncIterable

from grpc_service import GatewayService
from grpc_service.type import AgentMessage, AgentInfo, SessionStatus
from grpc_service import schema_pb2 as pb2

class GatewayHost(GatewayService):
    def __init__(self,
                 address: str,
                 gateway_id: str = None):
        super().__init__(address=address, gateway_id=gateway_id)
        
    async def _forward_agent_message(self, message: pb2.AgentMessage) -> AsyncIterable[pb2.AgentMessage]:
        """Route agent message to the receiver.

        Args:
            message (pb2.AgentMessage): The message to be routed.
        Returns:
            AsyncIterable[pb2.AgentMessage]: The response from the receiver.
        """
        stub = await super().get_node_stub(message.receiver_id)
        if not stub:
            print(f"<GW>: Routing failed: Receiver {message.receiver_id} not found")
            return

        try:
            # use the stub to call the remote method and route the message by stream
            stream = stub.CallAgent()
            await stream.write(message)

            # yield the responses from the stream
            async for response in stream:
                yield response

        except grpc.RpcError as e:
            receiver_info = await super().get_node_info(message.receiver_id)
            print(f"<GW>: Forwarding to Agent {message.receiver_id} (address: {receiver_info.address}) failed: {e.code()}")
            # delete failed node
            await super().deregister_node(message.receiver_id)
            return

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
        agent_dict = {node_id: info for node_id, info in self._registry.items() if isinstance(info, pb2.AgentInfo)}
        return agent_dict
    
    async def get_tools_info(self) -> Dict[str, AgentInfo]:
        """
        Get all registered tools.

        Returns:
            Dict[str, AgentInfo]: Dictionary of tool_id -> ToolInfo
        """
        tool_dict = {node_id: info for node_id, info in self._registry.items() if isinstance(info, pb2.ToolInfo)}
        return tool_dict