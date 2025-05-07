# -*- coding: utf-8 -*-
"""
Created on Sun Apr 27 16:25:37 2025

@author: clleng
"""

import asyncio
from typing import Dict, Union
from grpc_service import AgentServiceStub, GatewayServiceStub
from grpc_service.type import AgentMessage

class GatewaySession:
    """A Gateway session to manage one stream for communication between Agents"""
    def __init__(self, session_id: str=None, 
                 sender_id: str=None, 
                 receiver_id: str=None, 
                 stream_stream_call=None, 
                 timeout=1e9):
        """
        Initialize a new Gateway route session.

        Args:
            session_id (str): The session ID.
            sender_id (str): The sender ID.
            receiver_id (str): The receiver ID.
            stream_stream_call: The gRPC stream call object.
            timeout (float): Timeout for the session.
        """
        self.session_id = session_id
        self.sender_id = sender_id
        self.receiver_id = receiver_id
        self.stream_stream_call = stream_stream_call
        self.timeout = timeout
        self.forward_queue = asyncio.Queue()
        self.response_queue = asyncio.Queue()
        self._is_active = False
        self._forward_task = None
        self._response_task = None
        
    async def activate(self):
        self._is_active = True
        self._forward_task = asyncio.create_task(self._process_forward_queue())
        self._response_task = asyncio.create_task(self._process_incoming_responses())
    
    async def close(self):
        """Close the session."""
        self._is_active = False
        if self._forward_task:
            self._forward_task.cancel()
        if self._response_task:
            self._response_task.cancel()

    async def send(self, message: AgentMessage):
        """Send a message to stream."""
        try:
            # print(f"STREAM send message: {message.content[0]._text}")
            await self.stream_stream_call.write(message.to_grpc())
        except Exception as e:
            raise RuntimeError(f"Failed to send message: {e}")
        
    async def _process_forward_queue(self):
        """Forward messages from the send queue to the stream."""
        try:
            while self._is_active or not self.forward_queue.empty():
                try:
                    message = await asyncio.wait_for(self.forward_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                await self.send(message)
                self.forward_queue.task_done()
        except asyncio.CancelledError:
            # print(f"<GW session {self.session_id}> forward queue canceled")
            raise
        finally:
            pass
        
    async def enqueue_forward_message(self, message):
        """Put a message into the forward queue."""
        if self._is_active:
            await self.forward_queue.put(AgentMessage.from_grpc(message))
            # print(f"Enqueue message: {AgentMessage.from_grpc(message).content[0]._text} into forward queue")
            
    async def _process_incoming_responses(self):
        """Get incoming responses from stream and put them into response queue"""
        try:
            async for response in self.stream_stream_call:
                # print(f"Received response from stream: {AgentMessage.from_grpc(response).content[0]._text}")
                await self.response_queue.put(AgentMessage.from_grpc(response))
        except asyncio.CancelledError:
            # print(f"<GW session {self.session_id}> incoming queue canceled")
            raise
        finally:
            pass
    
    async def get_response(self):
        """Get a message from response queue"""
        while self._is_active or not self.response_queue.empty():
            response: AgentMessage = await self.response_queue.get()
            # print(f"Get response from queue: {response.content[0]._text}")
            yield response

class GatewaySessionMagager:
    """Session manager of Gateway host"""

    def __init__(self):
        self.route_sessions: Dict[str, GatewaySession] = {}
        self._lock = asyncio.Lock()

    async def create_or_get_session(self,
                                    session_id: str,
                                    sender_id: str,
                                    receiver_id: str,
                                    stub: Union[AgentServiceStub, GatewayServiceStub],
                                    callable_func: str="CallAgent") -> GatewaySession:
        """
        Create a new route session or return an existing one.
        Args:
            session_id (str): The original session ID.
            sender_id (str): The sender ID.
            receiver_id (str): The receiver ID.
            stub (Union[AgentServiceStub, GatewayServiceStub]): The gRPC stub for the session.
            callable_func (str): The function to call on the stub.
        Returns:
            GatewaySession: The session object.
        """
        async with self._lock:
            if session := self.route_sessions.get(session_id):
                return session
            stream_stream_call = getattr(stub, callable_func)()
            new_session = GatewaySession(session_id=session_id, 
                                         sender_id=sender_id, 
                                         receiver_id=receiver_id, 
                                         stream_stream_call=stream_stream_call)
            await new_session.activate()
            self.route_sessions[session_id] = new_session

            return new_session

    async def close_session(self, session_id: str):
        """
        Close a session by its ID.
        Args:
            session_id (str): The session ID.
        """
        async with self._lock:
            if session := self.route_sessions.pop(session_id, None):
                # Perform any necessary cleanup for the session
                await session.close()