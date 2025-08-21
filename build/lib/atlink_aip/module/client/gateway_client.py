# -*- coding: utf-8 -*-
"""
Created on Tues July 15 09:44:01 2025

@author: haixinwa
"""
import grpc
import uuid
import time
from typing import Optional, Union, List

from . import AgentClient
from . import ToolClient
from ...grpc_service import schema_pb2 as pb2
from ...grpc_service import GatewayServiceStub
from ...grpc_service.type import ToolBoxInfo, ToolResponse, AgentInfo, SessionStatus, TaskInfo, Mode, AgentSkill, TaskStatus, ContentItem, AgentMessage


class GatewayClient:
    """
    GatewayClient,
    """

    def __init__(self, gateway_address):
        """
        Initialize a new gateway client.

        Args:
            gateway_address: Address of gateway
        """
        self.gateway_address = gateway_address
        self.gateway_stub = None
        self._tool_clients = dict()
        self._agent_clients = dict()

    async def create_tool_client(self, toolbox_id: str):
        try:
            client = ToolClient(self.gateway_address, GatewayServiceStub, "RouteToolCalling")
            await client.start()

            if toolbox_id in self._tool_clients:
                raise RuntimeError(f"Failed to create tool client since the "
                                   f"(toolbox_id:{toolbox_id}) is already connected")

            self._tool_clients[toolbox_id] = client

        except Exception as e:
            raise RuntimeError(f"Failed to create tool client - Exception: {e}")

    async def create_agent_client(self, agent_id: str) -> Optional[str]:
        try:
            client = AgentClient(self.gateway_address, GatewayServiceStub, "RouteAgentCalling")
            session_id = await client.start()

            if (session_id, agent_id) in self._agent_clients:
                await client.close()
                raise RuntimeError(f"Failed to create agent client since the "
                                   f"(session id: {session_id} | agent_id:{agent_id}) is already connected")

            self._agent_clients[(session_id, agent_id)] = client

            return session_id

        except Exception as e:
            raise RuntimeError(f"<Agent>: Failed to create agent client - Exception: {e}")

    async def close_agent_client(self, session_id: str, agent_id: str):
        """
        Close a specific agent client.
        """
        key = (session_id, agent_id)
        client = self._agent_clients.pop(key, None)
        if client:
            await client.close()

    async def close_tool_client(self, toolbox_id: str):
        """
        Close a specific tool client.
        """
        client = self._tool_clients.pop(toolbox_id, None)
        if client:
            await client.close()

    async def start(self):
        channel = grpc.aio.insecure_channel(self.gateway_address)
        await channel.channel_ready()
        self.gateway_stub = GatewayServiceStub(channel)
        return self

    async def stop(self):
        for (session_id, receiver_id) in list(self._agent_clients.keys()):
            await self.close_agent_client(session_id, receiver_id)
            
        for toolbox_id in list(self._tool_clients.keys()):
            await self.close_tool_client(toolbox_id)

    def create_task_info(self, sender_id, parent_task_ids: List[str] = None) -> TaskInfo:
        """
        Create a new task info object.

        Args:
            parent_task_ids: List of parent task IDs (for task hierarchies)

        Returns:
            New TaskInfo object
        """
        task_id = f"task_{sender_id}_{time.strftime('%Y%m%d_%H%M%S')}"
        task_info = TaskInfo(
            task_id=task_id,
            parent_task_ids=parent_task_ids or [],
            task_status=TaskStatus.CREATE
        )
        return task_info

    def create_content_items(self,
                             content: Union[str, bytes, List[Union[str, bytes]]],
                             content_mode: Optional[Union[Mode, List[Mode]]] = None) -> List[ContentItem]:
        """
        Create a List of ContentItem object.
        Args:
            content: List of Contents or A Content. Type of each content can be string or bytes.
            content_mode: Mode of content: TEXT or IMAGE or AUDIO or EMBEDDED. It should correspond one-to-one with the content

        Returns:
            List[ContentItem1, ContentItem2, ...]
        """
        if isinstance(content, (str, bytes)):
            content = [content]
        if isinstance(content_mode, Mode):
            content_mode = [content_mode]

        content_items = list()
        if content_mode is None:  # All content share one Mode
            for c in content:
                if isinstance(c, str):
                    citem = ContentItem.write_text(c)
                else:
                    citem = ContentItem.write_embedded(c)
                content_items.append(citem)
        elif len(content_mode) == 1:  # All content share one Mode
            m = content_mode[0]
            if m == Mode.TEXT:
                for c in content:
                    assert isinstance(c, str), "Mode and content is not match"
                    citem = ContentItem.write_text(c)
                    content_items.append(citem)
            else:
                for c in content:
                    if isinstance(c, str):
                        citem = ContentItem.write_text(c)
                    else:
                        if m == Mode.IMAGE:
                            citem = ContentItem.write_image(c)
                        elif m == Mode.AUDIO:
                            citem = ContentItem.write_audio(c)
                        elif m == Mode.EMBEDDED:
                            citem = ContentItem.write_embedded(c)
                        else:
                            raise ("Illegal Mode definition")
                    content_items.append(citem)
        else:
            assert len(content) == len(content_mode), "Content and Mode is not one-to-one"
            for c, m in zip(content, content_mode):
                if m == Mode.TEXT:
                    assert isinstance(c, str), "Mode of string content must be Mode.TEXT"
                    citem = ContentItem.write_text(c)
                else:
                    assert isinstance(c, bytes), "Content with Mode of No-TEXT mush be bytes"
                    if m == Mode.IMAGE:
                        citem = ContentItem.write_image(c)
                    elif m == Mode.AUDIO:
                        citem = ContentItem.write_audio(c)
                    elif m == Mode.EMBEDDED:
                        citem = ContentItem.write_embedded(c)
                    else:
                        raise ("Illegal Mode definition")
                content_items.append(citem)

        return content_items

    def create_agent_message(self,
                             sender_id: str,
                             receiver_id: str,
                             content: Union[str, bytes, List[Union[str, bytes]]],
                             session_id: str = None,
                             task_info: TaskInfo = None,
                             content_mode: Optional[Union[Mode, List[Mode]]] = None,
                             session_status: SessionStatus = SessionStatus.START_QUEST,
                             message_id: str = None,
                             reply_to_message_id: str = None) -> AgentMessage:
        """
        Create an AgentMessage object.

        Args:
            receiver_id: ID of the agent to send the message to
            content: List of Content of the message
            session_id: Optional session ID (automatically generated if not provided)
            task_info: Optional task info
            content_mode: List of Content mode of the message
            session_status: Session status
            message_id: Optional message ID
            reply_to_message_id: Optional ID of the message this is replying to

        Returns:
            AgentMessage object
        """
        # Generate IDs if not provided
        if not message_id:
            message_id = f"msg_{uuid.uuid4().hex[:8]}"

        # Create task info if not provided
        if not task_info:
            task_info = self.create_task_info(sender_id)

        # Create multimodal grpc content
        content_items = self.create_content_items(content=content, content_mode=content_mode)

        # Create the message
        message = AgentMessage(
            sender_id=sender_id,
            receiver_id=receiver_id,
            content=content_items,
            session_id=session_id,
            session_status=session_status,
            task_info=task_info,
            message_id=message_id,
            reply_to_message_id=reply_to_message_id
        )

        return message

    async def get_gateway_nodes(self, sender_id, domain: str = 'default'):
        """
        Get the list of nodes registered with the gateway.

        Args:
            domain (str): The domain to query for nodes. Defaults to 'default'.
        """
        response = await self.gateway_stub.GetNodes(pb2.GetNodesRequest(agent_id=sender_id, domain=domain))
        peers = dict()
        for peer in response.peers:
            set_field = peer.WhichOneof("info_type")
            if set_field == "agent_info":
                agent_info = peer.agent_info
                peers.update({agent_info.agent_id: AgentInfo.from_grpc(agent_info)})
            elif set_field == "toolbox_info":
                toolbox_info = peer.toolbox_info
                peers.update({toolbox_info.toolbox_id: ToolBoxInfo.from_grpc(toolbox_info)})
            else:
                raise ValueError

        return peers

    async def send_tool_request(self,
                                sender_id: str,
                                receiver_id: str,
                                tool_name: str,
                                arguments: str) -> ToolResponse:
        client = self._tool_clients.get(receiver_id)
        if not client:
            raise RuntimeError(f"Not existed tool client (toolbox_id: {receiver_id})")

        response = await client.send_request(
            sender_id=sender_id,
            receiver_id=receiver_id,
            tool_name=tool_name,
            arguments=arguments
        )

        return response

    async def submit_agent_inquiry(self,
                                   sender_id: str,
                                   receiver_id: str,
                                   session_id: str,
                                   content: Union[str, bytes, List[Union[str, bytes]]],
                                   content_mode: Optional[Union[Mode, List[Mode]]] = None,
                                   session_status: SessionStatus = None,
                                   task_info: TaskInfo = None):
        client = self._agent_clients.get((session_id, receiver_id))
        if not client:
            raise RuntimeError(f"Not existed agent client (session id: {session_id} | receiver_id: {receiver_id})")

        # Determine session status based on client existence and provided status
        if session_status is None:
            if not client.occupied:
                session_status = SessionStatus.START_QUEST  # New session
            else:
                session_status = SessionStatus.HOLD_QUEST  # Continue existing session
        elif session_status == SessionStatus.START_QUEST:
            if client.occupied:
                raise RuntimeError(f"Cannot start quest in an occupied client session")
        elif session_status in [SessionStatus.HOLD_QUEST, SessionStatus.STOP_QUEST]:
            if not client.occupied:
                raise RuntimeError(f"Cannot send {session_status} in an unoccupied client session")
        else:
            raise RuntimeError(f"Not supported session status: {session_status}")

        # Create task info if not provided
        if not task_info:
            task_info = self.create_task_info(sender_id)

        # Create and send the message
        message = self.create_agent_message(
            sender_id=sender_id,
            receiver_id=receiver_id,
            content=content,
            session_id=session_id,
            task_info=task_info,
            content_mode=content_mode,
            session_status=session_status
        )

        await  client.send_request(message)

    async def receive_agent_feedback(self, session_id: str, receiver_id: str) -> AgentMessage:
        """
        get a feedback from another agent server.
        """
        client = self._agent_clients.get((session_id, receiver_id))
        if not client:
            raise RuntimeError(f"Not existed agent client (session id: {session_id} | receiver_id: {receiver_id})")

        response = await client.get_response()
        if response.session_status == SessionStatus.STOP_RESPONSE:
            await self.close_agent_client(session_id, receiver_id)

        return response