# -*- coding: utf-8 -*-
"""
Created on Fri Apr 21 13:00:00 2025

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
    VIDEO = 3
    NONE = 4


class MessageType(Enum):
    TASK_QUEST = 0
    TASK_RESPONSE = 1


class Status(Enum):
    EXECUTING = 0
    WAITING = 1
    BREAK = 2
    FINISH = 3


class SessionStatus(Enum):
    START_QUEST = 0
    START_RESPONSE = 1
    STOP_QUEST = 2
    STOP_RESPONSE = 3


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
    def __init__(self, task_id: str, parent_task_ids: List[str],
                 message_type: MessageType, status: Status):
        self.task_id = task_id
        self.parent_task_ids = parent_task_ids
        self.message_type = message_type
        self.status = status

    def to_grpc(self) -> pb2.TaskInfo:
        return pb2.TaskInfo(
            task_id=self.task_id,
            parent_task_ids=self.parent_task_ids,
            message_type=convert_enum(self.message_type, pb2.TaskInfo.MessageType),
            status=convert_enum(self.status, pb2.TaskInfo.Status)
        )

    @classmethod
    def from_grpc(cls, grpc_obj: pb2.TaskInfo) -> 'TaskInfo':
        return cls(
            task_id=grpc_obj.task_id,
            parent_task_ids=list(grpc_obj.parent_task_ids),
            message_type=restore_enum(grpc_obj.message_type, MessageType),
            status=restore_enum(grpc_obj.status, Status)
        )


class Peer:
    def __init__(self):
        self._agent_info: Optional[AgentInfo] = None
        self._tool_info: Optional[ToolInfo] = None

    def to_grpc(self) -> pb2.Peer:
        grpc_peer = pb2.Peer()
        if self._agent_info is not None:
            grpc_peer.agent_info.CopyFrom(self._agent_info.to_grpc())
        elif self._agent_info is not None:
            grpc_peer.tool_info.CopyFrom(self._agent_info.to_grpc())
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
    def __init__(self, agent_id: str, address: str, name: str, domain: str,
                 input_mode: Mode, output_mode: Mode, description: str,
                 skills: List[AgentSkill], version: str):
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
            input_mode=convert_enum(self.input_mode, pb2.Mode),
            output_mode=convert_enum(self.output_mode, pb2.Mode),
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
            input_mode=restore_enum(grpc_obj.input_mode, Mode),
            output_mode=restore_enum(grpc_obj.output_mode, Mode),
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
    def __init__(self, tool_id: str, address: str, name: str, domain: str,
                 input_mode: Mode, output_mode: Mode, description: str,
                 arguments: Dict[str, str], version: str):
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
            input_mode=convert_enum(self.input_mode, pb2.Mode),
            output_mode=convert_enum(self.output_mode, pb2.Mode),
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
            input_mode=restore_enum(grpc_obj.input_mode, Mode),
            output_mode=restore_enum(grpc_obj.output_mode, Mode),
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
    def __init__(self, sender_id: str, receiver_id: str, session_id: str,
                 session_status: SessionStatus, task: TaskInfo,
                 content: Union[str, bytes], content_mode: Mode,
                 message_id: str, reply_to_message_id: Optional[str] = None):
        self.sender_id = sender_id
        self.receiver_id = receiver_id
        self.session_id = session_id
        self.session_status = session_status
        self.task = task
        self.content = content
        self.content_mode = content_mode
        self.message_id = message_id
        self.reply_to_message_id = reply_to_message_id

    def to_grpc(self) -> pb2.AgentMessage:
        grpc_obj = pb2.AgentMessage(
            sender_id=self.sender_id,
            receiver_id=self.receiver_id,
            session_id=self.session_id,
            session_status=convert_enum(self.session_status, pb2.AgentMessage.SessionStatus),
            task=self.task.to_grpc(),
            content_mode=convert_enum(self.content_mode, pb2.Mode),
            message_id=self.message_id
        )
        if isinstance(self.content, str):
            grpc_obj.text = self.content
        elif isinstance(self.content, bytes):
            grpc_obj.binary = self.content
        if self.reply_to_message_id:
            grpc_obj.reply_to_message_id = self.reply_to_message_id
        return grpc_obj

    @classmethod
    def from_grpc(cls, grpc_obj: pb2.AgentMessage) -> 'AgentMessage':
        content = grpc_obj.text if grpc_obj.HasField("text") else grpc_obj.binary
        return cls(
            sender_id=grpc_obj.sender_id,
            receiver_id=grpc_obj.receiver_id,
            session_id=grpc_obj.session_id,
            session_status=restore_enum(grpc_obj.session_status, SessionStatus),
            task=TaskInfo.from_grpc(grpc_obj.task),
            content=content,
            content_mode=restore_enum(grpc_obj.content_mode, Mode),
            message_id=grpc_obj.message_id,
            reply_to_message_id=grpc_obj.reply_to_message_id if grpc_obj.reply_to_message_id else None
        )


class ToolRequest:
    def __init__( self, sender_id: str, receiver_id: str, session_id: str,
                  tool_name: str, arguments: Dict[str, str]):
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
    def __init__(self, sender_id: str, receiver_id: str, session_id: str,
                 success: bool, content: Union[str, bytes, bytes, bytes],
                 error_message: str = ""):
        self.sender_id = sender_id
        self.receiver_id = receiver_id
        self.session_id = session_id
        self.success = success
        self.content = content
        self.error_message = error_message

    def to_grpc(self) -> pb2.ToolResponse:
        grpc_obj = pb2.ToolResponse(
            sender_id=self.sender_id,
            receiver_id=self.receiver_id,
            session_id=self.session_id,
            success=self.success,
            error_message=self.error_message
        )
        if isinstance(self.content, str):
            grpc_obj.text = self.content
        elif isinstance(self.content, bytes):
            content_type = "image"
            getattr(grpc_obj, content_type).value = self.content

        return grpc_obj

    @classmethod
    def from_grpc(cls, grpc_obj: pb2.ToolResponse) -> 'ToolResponse':
        content = ""
        if grpc_obj.HasField("text"):
            content = grpc_obj.text
        elif grpc_obj.HasField("image"):
            content = grpc_obj.image
        elif grpc_obj.HasField("audio"):
            content = grpc_obj.audio
        elif grpc_obj.HasField("embedded"):
            content = grpc_obj.embedded
        return cls(
            sender_id=grpc_obj.sender_id,
            receiver_id=grpc_obj.receiver_id,
            session_id=grpc_obj.session_id,
            success=grpc_obj.success,
            content=content,
            error_message=grpc_obj.error_message
        )
