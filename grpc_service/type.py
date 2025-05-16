# -*- coding: utf-8 -*-
"""
Created on Mon Apr 21 13:00:00 2025

@author: haixinwa
"""

# -*- coding: utf-8 -*-

from enum import Enum
from typing import List, Dict, Union, Optional, Type, Any
from . import schema_pb2 as pb2


# -------------------- Enum Define--------------------
def convert_enum(py_enum: Enum, grpc_enum_class: Type) -> Any:
    """Convert Python enumerations to gRPC enumeration values"""
    return grpc_enum_class.Value(py_enum.name)


def restore_enum(grpc_enum_value: int, py_enum_class: Type[Enum]) -> Enum:
    """Recover Python enumerations from gRPC enumeration values"""
    return py_enum_class(grpc_enum_value)


class Mode(Enum):
    TEXT = 0
    IMAGE = 1
    AUDIO = 2
    EMBEDDED = 3


class SessionStatus(Enum):
    START_QUEST = 0
    HOLD_QUEST = 1
    HOLD_RESPONSE = 2
    STOP_QUEST = 3
    STOP_RESPONSE = 4


class TaskStatus(Enum):
    CREATE=0
    EXECUTING = 1
    WAITING = 2
    BREAK = 3
    FINISH = 4


# -------------------- Message Define--------------------
class AgentSkill:
    def __init__(self, skill_id: str, capability: str):
        self.skill_id = skill_id
        self.capability = capability

    def to_grpc(self) -> pb2.AgentInfo.AgentSkill:
        return pb2.AgentInfo.AgentSkill(
            skill_id=self.skill_id,
            capability=self.capability
        )

    @classmethod
    def from_grpc(cls, grpc_obj: pb2.AgentInfo.AgentSkill) -> 'AgentSkill':
        return cls(
            skill_id=grpc_obj.skill_id,
            capability=grpc_obj.capability
        )


class TaskInfo:
    def __init__(
        self,
        task_id: str = "",
        parent_task_ids: List[str] = list(),
        task_status: TaskStatus = TaskStatus.CREATE
    ):
        self.task_id = task_id
        self.parent_task_ids = parent_task_ids
        self.task_status = task_status

    def to_grpc(self) -> pb2.TaskInfo:
        return pb2.TaskInfo(
            task_id=self.task_id,
            parent_task_ids=self.parent_task_ids,
            task_status=convert_enum(self.task_status, pb2.TaskInfo.TaskStatus)
        )

    @classmethod
    def from_grpc(cls, grpc_obj: pb2.TaskInfo) -> 'TaskInfo':
        return cls(
            task_id=grpc_obj.task_id,
            parent_task_ids=list(grpc_obj.parent_task_ids),
            task_status=restore_enum(grpc_obj.task_status, TaskStatus)
        )


class ContentItem:
    def __init__(
        self,
        text: Optional[str] = None,
        image: Optional[bytes] = None,
        audio: Optional[bytes] = None,
        embedded: Optional[bytes] = None
    ):
        self._text: Optional[str] = text
        self._image: Optional[bytes] = image
        self._audio: Optional[bytes] = audio
        self._embedded: Optional[bytes] = embedded

    def to_grpc(self) -> pb2.ContentItem:
        grpc_item = pb2.ContentItem()
        if self._text is not None:
            grpc_item.text = self._text
        elif self._image is not None:
            grpc_item.image = self._image
        elif self._audio is not None:
            grpc_item.audio = self._audio
        elif self._embedded is not None:
            grpc_item.embedded = self._embedded

        return grpc_item

    @classmethod
    def from_grpc(cls, grpc_item: pb2.ContentItem) -> 'ContentItem':
        item = cls()
        which = grpc_item.WhichOneof("data")
        if which == "text":
            item._text = grpc_item.text
        elif which == "image":
            item._image = grpc_item.image
        elif which == "audio":
            item._audio = grpc_item.audio
        elif which == "embedded":
            item._embedded = grpc_item.embedded

        return item

    @classmethod
    def write_text(cls, str):
        item = cls()
        item._text = str

        return item

    @classmethod
    def write_image(cls, bytes):
        item = cls()
        item._image = bytes

        return item

    @classmethod
    def write_audio(cls, bytes):
        item = cls()
        item._audio = bytes

        return item

    @classmethod
    def write_embedded(cls, bytes):
        item = cls()
        item._embedded = bytes

        return item

class Peer:
    def __init__(self):
        self._agent_info: Optional[AgentInfo] = None
        self._tool_info: Optional[ToolInfo] = None

    def to_grpc(self) -> pb2.Peer:
        grpc_peer = pb2.Peer()
        if self._agent_info is not None:
            grpc_peer.agent_info.CopyFrom(self._agent_info.to_grpc())
        elif self._tool_info is not None:
            grpc_peer.tool_info.CopyFrom(self._tool_info.to_grpc())
        return grpc_peer

    @classmethod
    def from_grpc(cls, grpc_peer: pb2.Peer) -> 'Peer':
        peer = cls()
        which = grpc_peer.WhichOneof("info_type")
        if which == "agent_info":
            peer.agent_info = AgentInfo.from_grpc(grpc_peer.agent_info)
        elif which == "tool_info":
            peer.tool_info = ToolInfo.from_grpc(grpc_peer.tool_info)
        return peer


class AgentInfo:
    def __init__(
        self,
        agent_id: str = "",
        address: str = "",
        name: str = "",
        domain: str = "",
        input_mode: List[Mode] = None,
        output_mode: List[Mode] = None,
        description: str = "",
        skills: List[AgentSkill] = None,
        version: str = ""
    ):
        self.agent_id = agent_id
        self.address = address
        self.name = name
        self.domain = domain
        self.input_mode = input_mode
        self.output_mode = output_mode
        self.description = description
        self.skills = skills
        self.version = version

    def to_grpc(self) -> pb2.AgentInfo:
        grpc_obj = pb2.AgentInfo(
            agent_id=self.agent_id,
            address=self.address,
            name=self.name,
            domain=self.domain,
            input_mode=[convert_enum(_mode, pb2.Mode) for _mode in self.input_mode],
            output_mode=[convert_enum(_mode, pb2.Mode) for _mode in self.output_mode],
            description=self.description,
            skills=[s.to_grpc() for s in self.skills],
            version=self.version
        )
        return grpc_obj

    @classmethod
    def from_grpc(cls, grpc_obj: pb2.AgentInfo) -> 'AgentInfo':
        return cls(
            agent_id=grpc_obj.agent_id,
            address=grpc_obj.address,
            name=grpc_obj.name,
            domain=grpc_obj.domain,
            input_mode=[restore_enum(_mode, Mode) for _mode in grpc_obj.input_mode],
            output_mode=[restore_enum(_mode, Mode) for _mode in grpc_obj.input_mode],
            description=grpc_obj.description,
            skills=[AgentSkill.from_grpc(s) for s in grpc_obj.skills],
            version=grpc_obj.version
        )


class RegisterAgentResponse:
    def __init__(
        self,
        success: bool,
        peers: List[Peer]
    ):
        self.success = success
        self.peers = peers

    def to_grpc(self) -> pb2.RegisterAgentResponse:
        return pb2.RegisterAgentResponse(
            success = self.success,
            peers = [peer.to_grpc() for peer in self.peers]
        )

    @classmethod
    def from_grpc(cls, grpc_obj: pb2.RegisterAgentResponse) -> 'RegisterAgentResponse':
        return cls(
            success=grpc_obj.success,
            peers=[Peer.from_grpc(peer) for peer in grpc_obj.peers]
        )


class ToolInfo:
    def __init__(
        self,
        tool_id: str = "",
        address: str = "",
        name: str = "",
        domain: str = "",
        input_mode: List[Mode] = None,
        output_mode: List[Mode] = None,
        description: str = "",
        arguments: Dict[str, str] = None,
        version: str = ""
    ):
        self.tool_id = tool_id
        self.address = address
        self.name = name
        self.domain = domain
        self.input_mode = input_mode
        self.output_mode = output_mode
        self.description = description
        self.arguments = arguments
        self.version = version

    def to_grpc(self) -> pb2.ToolInfo:
        return pb2.ToolInfo(
            tool_id=self.tool_id,
            address=self.address,
            name=self.name,
            domain=self.domain,
            input_mode=[convert_enum(_mode, pb2.Mode) for _mode in self.input_mode],
            output_mode=[convert_enum(_mode, pb2.Mode) for _mode in self.output_mode],
            description=self.description,
            arguments=self.arguments,
            version=self.version
        )

    @classmethod
    def from_grpc(cls, grpc_obj: pb2.ToolInfo) -> 'ToolInfo':
        return cls(
            tool_id=grpc_obj.tool_id,
            address=grpc_obj.address,
            name=grpc_obj.name,
            domain=grpc_obj.domain,
            input_mode=[restore_enum(_mode, Mode) for _mode in grpc_obj.input_mode],
            output_mode=[restore_enum(_mode, Mode) for _mode in grpc_obj.input_mode],
            description=grpc_obj.description,
            arguments=dict(grpc_obj.arguments),
            version=grpc_obj.version
        )


class RegisterToolResponse:
    def __init__(self, success: bool):
        self.success = success

    def to_grpc(self) -> pb2.RegisterToolResponse:
        return pb2.RegisterToolResponse(
            success = self.success
        )

    @classmethod
    def from_grpc(cls, grpc_obj: pb2.RegisterToolResponse) -> 'RegisterToolResponse':
        return cls(
            success=grpc_obj.success
        )
        
    
class DeregisterNodeRequest:
    def __init__(self, node_id: str):
        self.node_id = node_id

    def to_grpc(self) -> pb2.DeregisterNodeRequest:
        return pb2.DeregisterNodeRequest(
            node_id = self.node_id
        )

    @classmethod
    def from_grpc(cls, grpc_obj: pb2.DeregisterNodeRequest) -> 'DeregisterNodeRequest':
        return cls(
            node_id=grpc_obj.node_id
        )
        
        
class DeregisterNodeResponse:
    def __init__(self, success: bool):
        self.success = success

    def to_grpc(self) -> pb2.DeregisterNodeResponse:
        return pb2.DeregisterToolResponse(
            success = self.success
        )

    @classmethod
    def from_grpc(cls, grpc_obj: pb2.DeregisterNodeResponse) -> 'DeregisterNodeResponse':
        return cls(
            success=grpc_obj.success
        )

class GetNodesRequest:
    def __init__(self, agent_id: str, domain: str):
        self.agent_id = agent_id
        self.domain = domain

    def to_grpc(self) -> pb2.GetNodesRequest:
        return pb2.GetNodesRequest(
            agent_id = self.agent_id,
            domain = self.domain
        )

    @classmethod
    def from_grpc(cls, grpc_obj: pb2.GetNodesRequest) -> 'GetNodesRequest':
        return cls(
            agent_id=grpc_obj.agent_id,
            domain=grpc_obj.domain
        )


class GetNodesResponse:
    def __init__(self, peers: List[Peer]):
        self.peers = peers

    def to_grpc(self) -> pb2.GetNodesResponse:
        return pb2.GetNodesResponse(
            peers = [peer.to_grpc() for peer in self.peers]
        )

    @classmethod
    def from_grpc(cls, grpc_obj: pb2.GetNodesResponse) -> 'GetNodesResponse':
        return cls(
            peers=[Peer.from_grpc(peer) for peer in grpc_obj.peers]
        )


class AgentMessage:
    def __init__(
        self,
        sender_id: str = "",
        receiver_id: str = "",
        session_id: str = "",
        session_status: SessionStatus = SessionStatus.START_QUEST,
        task_info: TaskInfo = TaskInfo(),
        message_id: str = "",
        reply_to_message_id: str = "",
        content: List[ContentItem] = list()
    ):
        self.sender_id = sender_id
        self.receiver_id = receiver_id
        self.session_id = session_id
        self.session_status = session_status
        self.task_info = task_info
        self.message_id = message_id
        self.reply_to_message_id = reply_to_message_id
        self.content = content

    def to_grpc(self) -> pb2.AgentMessage:
        grpc_obj = pb2.AgentMessage(
            sender_id=self.sender_id,
            receiver_id=self.receiver_id,
            session_id=self.session_id,
            session_status=convert_enum(self.session_status, pb2.AgentMessage.SessionStatus),
            task_info=self.task_info.to_grpc(),
            message_id=self.message_id,
            reply_to_message_id=self.reply_to_message_id,
            content=[item.to_grpc() for item in self.content]
        )

        return grpc_obj

    @classmethod
    def from_grpc(cls, grpc_obj: pb2.AgentMessage) -> 'AgentMessage':
        return cls(
            sender_id=grpc_obj.sender_id,
            receiver_id=grpc_obj.receiver_id,
            session_id=grpc_obj.session_id,
            session_status=restore_enum(grpc_obj.session_status, SessionStatus),
            task_info=TaskInfo.from_grpc(grpc_obj.task_info),
            message_id=grpc_obj.message_id,
            reply_to_message_id=grpc_obj.reply_to_message_id,
            content=[ContentItem.from_grpc(item) for item in grpc_obj.content]
        )

    def add_content(self, item: ContentItem):
        if not self.content:
            self.content = list()

        self.content.append(item)


class ToolRequest:
    def __init__(
        self,
        sender_id: str = "",
        receiver_id: str = "",
        session_id: str = "",
        tool_name: str = "",
        arguments: Dict[str, str] = dict()
    ):
        self.sender_id = sender_id
        self.receiver_id = receiver_id
        self.session_id = session_id
        self.tool_name = tool_name
        self.arguments = arguments

    def to_grpc(self) -> pb2.ToolRequest:
        grpc_obj = pb2.ToolRequest(
            sender_id=self.sender_id,
            receiver_id=self.receiver_id,
            session_id=self.session_id,
            tool_name=self.tool_name,
            arguments=self.arguments
        )

        return grpc_obj

    @classmethod
    def from_grpc(cls, grpc_obj: pb2.ToolRequest) -> 'ToolRequest':
        return cls(
            sender_id=grpc_obj.sender_id,
            receiver_id=grpc_obj.receiver_id,
            session_id=grpc_obj.session_id,
            tool_name=grpc_obj.tool_name,
            arguments=grpc_obj.arguments
        )


class ToolResponse:
    def __init__(
        self,
        sender_id: str = "",
        receiver_id: str = "",
        session_id: str ="",
        is_error: bool = False,
        error_message: str = "",
        content: List[ContentItem] = list()
    ):
        self.sender_id = sender_id
        self.receiver_id = receiver_id
        self.session_id = session_id
        self.is_error = is_error
        self.error_message = error_message
        self.content = content

    def to_grpc(self) -> pb2.ToolResponse:
        grpc_obj = pb2.ToolResponse(
            sender_id=self.sender_id,
            receiver_id=self.receiver_id,
            session_id=self.session_id,
            is_error=self.is_error,
            error_message=self.error_message,
            content=[item.to_grpc() for item in self.content]
        )

        return grpc_obj

    @classmethod
    def from_grpc(cls, grpc_obj: pb2.ToolResponse) -> 'ToolResponse':
        return cls(
            sender_id=grpc_obj.sender_id,
            receiver_id=grpc_obj.receiver_id,
            session_id=grpc_obj.session_id,
            is_error=grpc_obj.is_error,
            error_message=grpc_obj.error_message,
            content=[ContentItem.from_grpc(item) for item in grpc_obj.content]
        )

    def add_content(self, item: ContentItem):
        if not self.content:
            self.content = list()

        self.content.append(item)