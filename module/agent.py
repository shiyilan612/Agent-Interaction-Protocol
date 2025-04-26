# -*- coding: utf-8 -*-
"""
Created on Thu Apr 24 09:44:01 2025

@author: xmkang
"""

import asyncio
import uuid
import time
from typing import Dict, List, Optional, Callable, Any, Union

from grpc_service.type import AgentInfo, TaskInfo, AgentMessage, ToolRequest, ToolResponse
from grpc_service.type import AgentSkill, SessionStatus, TaskStatus, Mode, ContentItem
from grpc_service import AgentServiceStub, GatewayServiceStub, ToolServiceStub
from module.client import AgentClient
from module.server import AgentServer
from module.client import ToolClient


class Agent:
    """
    A high-level Agent module that wraps the AgentServer, AgentClient, ToolClient implementations,
    """
    
    def __init__(self, 
                 address: str,
                 agent_id: str = None,
                 name: str = None,
                 domain: str = "default",
                 description: str = "",
                 version: str = "1.0.0",
                 input_mode: List[Mode] = [Mode.TEXT],
                 output_mode: List[Mode] = [Mode.TEXT],
                 skills: List[AgentSkill] = None):
        """
        Initialize a new Agent.
        
        Args:
            address: Address where this agent will be hosted (e.g., "localhost:50051")
            agent_id: Unique identifier for this agent (defaults to UUID if not provided)
            name: Human-readable name for this agent
            domain: Agent group/domain 
            description: Detailed description of the agent's functionality
            version: Agent version
            input_mode: Expected input modality (TEXT, IMAGE, etc.)
            output_mode: Output modality provided by the agent
            skills: List of skills this agent possesses
        """
        self.address = address
        self.agent_id = agent_id if agent_id else f"agent_{str(uuid.uuid4())}"
        self.name = name if name else self.agent_id
        self.domain = domain
        self.description = description
        self.version = version
        self.input_mode = input_mode
        self.output_mode = output_mode
        self.skills = skills or []
        
        # Create agent info
        self.agent_info = self._create_agent_info()
        
        # Server instance
        self._server = None
        
        # Store active client connections
        self._agent_clients: Dict[str, AgentClient] = {}
        self._tool_clients: Dict[str, ToolClient] = {}
        
        # Gateway connection
        self._gateway_address = None
        
        # Message processing function
        self._process_request_func = None
        self._process_response_func = None
        
        # Task management
        self._tasks: Dict[str, TaskInfo] = {}
        self._task_counter = 0
        
    def _create_agent_info(self) -> AgentInfo:
        """Create an AgentInfo object for registration with the gateway."""
        agent_info = AgentInfo(
            agent_id=self.agent_id,
            address=self.address,
            name=self.name,
            domain=self.domain,
            input_mode=self.input_mode,
            output_mode=self.output_mode,
            description=self.description,
            version=self.version,
            skills=self.skills
        )
        return agent_info
    
    def set_process_request_handler(self, handler: Callable):
        """
        Set the function to handle incoming requests.
        
        Args:
            handler: A callable that processes AgentMessage requests and returns AgentMessage responses
        """
        self._process_request_func = handler
        return self
    
    def set_process_response_handler(self, handler: Callable):
        """
        Set the function to handle incoming responses.
        
        Args:
            handler: A callable that processes AgentMessage responses
        """
        self._process_response_func = handler
        return self
    
    # async def _default_process_request_handler(self, message: AgentMessage) -> AgentMessage:
    #     """Default message processing function if none is provided."""

    
    # async def _default_process_response_handler(self, message: AgentMessage):
    #     """Default response processing function if none is provided."""
    
    async def start(self):
        """Start the agent server."""
        # if not self._process_request_func:
        #     self._process_request_func = self._default_process_request_handler
            
        # if not self._process_response_func:
        #     self._process_response_func = self._default_process_response_handler
        
        self._server = AgentServer(self.agent_info, self._process_request_func)
        await self._server.start()
        return self
    
    async def stop(self):
        """Stop the agent server and close all client connections."""
        # Close all active client connections
        for client_id, client in list(self._agent_clients.items()):
            await client.close()
            del self._agent_clients[client_id]
            
        for tool_id, tool_client in list(self._tool_clients.items()):
            await tool_client.close()
            del self._tool_clients[tool_id]
        
        # Stop the server
        if self._server:
            await self._server.stop()
            self._server = None
            
        # Clear task info
        self._tasks.clear()
    
    async def register_to_gateway(self, gateway_address: str):
        """Register to the gateway service."""
        self._gateway_address = gateway_address
        if not self._server:
            raise RuntimeError("Agent server not started. Call start() first.")
            
        await self._server.connect_to_gateway(gateway_address)
        
        return self
    
    async def update_peers(self, domain: str = 'default'):
        """
        Update the list of peers from the gateway.
        
        Args:
            domain: The domain to query for peers
        """
        if not self._gateway_address:
            raise RuntimeError("Not connected to gateway")
            
        if self._server:
            await self._server.get_gateway_node(domain)
        return self
    
    async def get_nodes(self) -> Dict[str, Union[AgentInfo, Any]]:
        """
        get the list of peers.
        """
        if not self._gateway_address:
            raise RuntimeError("Not connected to gateway")
            
        if self._server:
            return self._server._peers
        return {}
    
    
    def create_task_info(self, parent_task_ids: List[str] = None) -> TaskInfo:
        """
        Create a new task info object.
        
        Args:
            parent_task_ids: List of parent task IDs (for task hierarchies)
            
        Returns:
            New TaskInfo object
        """
        self._task_counter += 1
        task_id = f"task_{self.agent_id}_{time.strftime('%Y%m%d_%H%M%S')}_{self._task_counter}"
        
        task_info = TaskInfo(
            task_id=task_id,
            parent_task_ids=parent_task_ids or [],
            task_status=TaskStatus.CREATE
        )
        
        self._tasks[task_id] = task_info
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
        if content_mode is None: #All content share one Mode
            for c in content:
                if isinstance(c, str):
                    citem = ContentItem.write_text(c)
                else:
                    citem = ContentItem.write_embedded(c)
                content_items.append(citem)
        elif len(content_mode) == 1: #All content share one Mode
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
                            raise("Illegal Mode definition")
                    content_items.append(citem)
        else:
            assert len(content) == len(content_mode), "Content and Mode is not one-to-one"
            for c, m in zip(content, content_mode):
                if m == Mode.TEXT:
                    assert isinstance(c, str), "Mode of string content must be Mode.TEXT"
                    citem= ContentItem.write_text(c)
                else:
                    assert isinstance(c, bytes), "Content with Mode of No-TEXT mush be bytes"
                    if m == Mode.IMAGE:
                        citem = ContentItem.write_image(c)
                    elif m == Mode.AUDIO:
                        citem = ContentItem.write_audio(c)
                    elif m == Mode.EMBEDDED:
                        citem = ContentItem.write_embedded(c)
                    else:
                        raise("Illegal Mode definition")
                content_items.append(citem)
        
        return content_items
            
    
    def create_agent_message(self, 
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
            task_info = self.create_task_info()
        
        #Create multimodal grpc content
        content_items = self.create_content_items(content=content, content_mode=content_mode)
        
        # Create the message
        message = AgentMessage(
            sender_id=self.agent_id,
            receiver_id=receiver_id,
            content=content_items,
            session_id=session_id or f"session_{uuid.uuid4().hex[:8]}",
            session_status=session_status,
            task_info=task_info,
            message_id=message_id,
            reply_to_message_id=reply_to_message_id
        )
        
        return message
    
    async def send_message(self, 
                          receiver_id: str, 
                          content: Union[str, bytes, List[Union[str, bytes]]],
                          session_id: str = None,
                          task_info: TaskInfo = None,
                          content_mode: Optional[Union[Mode, List[Mode]]] = None,
                          session_status: SessionStatus = None) -> AgentClient:
        """
        Send a message to another agent through the gateway.
        
        Args:
            receiver_id: ID of the agent to send the message to
            content: Content of the message
            session_id: Optional session ID (automatically generated if not provided)
            task_info: Optional task info (automatically created if not provided)
            content_mode: Content mode of the message
            session_status: Session status to use (START_QUEST, HOLD_QUEST, or STOP_QUEST)
                        If None, will auto-detect based on session existence
            
        Returns:
            The AgentClient instance associated with this session
        """
        if not self._gateway_address:
            raise RuntimeError("Not connected to gateway")
        
        # Check if we already have an active client for this receiver
        client = self._agent_clients.get(receiver_id)
        
        # Determine session status based on client existence and provided status
        if session_status is None:
            if not client:
                session_status = SessionStatus.START_QUEST  # New session
            else:
                session_status = SessionStatus.HOLD_QUEST  # Continue existing session
                
        # Create a new client if none exists or if explicitly starting a new session
        if not client or session_status == SessionStatus.START_QUEST:
            # Create a new client and connect to the gateway
            client = await AgentClient(self._process_response_func).start(
                self._gateway_address, 
                GatewayServiceStub,
                "RouteAgentCalling"
            )
            self._agent_clients[receiver_id] = client
        

            
        # Create task info if not provided
        if not task_info:
            task_info = self.create_task_info()
        
        # Create and send the message
        message = self.create_agent_message(
            receiver_id=receiver_id,
            content=content,
            session_id=session_id or client.session.session_id,
            task_info=task_info,
            content_mode=content_mode,
            session_status=session_status
        )
        
        await client.send_message(message)
        return client
    

    def create_tool_request(self, 
                           receiver_id: str, 
                           session_id: str = None,
                           tool_name: str = None,
                           arguments: Dict[str, Any] = None) -> ToolRequest:
        """
        Create an AgentMessage object.
        
        Args:
            receiver_id: ID of the tool to call.
            session_id: Optional session ID (automatically generated if not provided)
            tool_name: Tool name
            arguments: Arguments for the tool call.
            
        Returns:
            ToolRequest object
        """

        #Ensure all key-value in arguments is string
        arguments = {str(k): str(v if v is not None else "N/A") for k, v in arguments.items()}
            
        # Create the requst
        request = ToolRequest(
            sender_id=self.agent_id,
            receiver_id=receiver_id,
            session_id=session_id or f"session_{uuid.uuid4().hex[:8]}",
            tool_name=tool_name or "unknown",
            arguments=arguments or {}
        )
        
        return request
    


    async def call_tool(self, 
                        tool_id: str, 
                        session_id: str = None,
                        tool_name: str = None,
                        arguments: Dict[str, Any] = None) -> ToolResponse:
        """
        Call a tool with the given ID through gateway.
        
        Args:
            tool_id (str): ID of the tool to call.
            session_id: Optional session ID (automatically generated if not provided)
            arguments (Dict[str, Any], optional): Arguments for the tool call.
            
        Returns:
            ToolResponse: The response from the tool.
        """
        
        if not self._gateway_address:
            raise RuntimeError("Not connected to gateway")
            
            
        # Check if we already have an tool client for this receiver tool
        tool_client = self._tool_clients.get(tool_id)
        
        # Create a new client if none exists
        if not tool_client:
            # Create a new tool client and connect to the gateway
            tool_client = await ToolClient().start(
                self._gateway_address,
                GatewayServiceStub,
                "RouteToolCalling"
            )
            self._tool_clients[tool_id] = tool_client
                
        # Create tool request
        request = self.create_tool_request(
                receiver_id=tool_id,
                session_id=session_id or tool_client.session.session_id,
                tool_name=tool_name,
                arguments=arguments if arguments else {}
            )
        
        try:
            # Send the request and get the response
            response = await tool_client.send_request(request)
            return response
                
        except TimeoutError:
            # Handle timeout specifically
            print(f"Tool call to {tool_id} timed out")
            raise
        except Exception as e:
            # Handle other exceptions
            print(f"Error calling tool {tool_id}: {str(e)}")
            raise
            
    async def close_agent_client(self, receiver_id: str) -> bool:
        """
        Close a specific agent client.
        
        Args:
            receiver_id: ID of the receiver agent
            
        Returns:
            True if session was closed, False if not found
        """
        if receiver_id in self._agent_clients:
            client = self._agent_clients.pop(receiver_id)
            await client.close()
            return True
        return False
    
    async def close_tool_client(self, tool_id: str) -> bool:
        """
        Close a specific tool client.
        
        Args:
            tool_id: ID of the tool
            
        Returns:
            True if client was closed, False if not found
        """
        if tool_id in self._tool_clients:
            client = self._tool_clients.pop(tool_id)
            await client.close()
            return True
        return False
