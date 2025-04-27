# -*- coding: utf-8 -*-
"""
Created on Sun Apr 27 16:25:37 2025

@author: clleng
"""

import asyncio
from typing import Dict
from grpc_service import AgentServiceStub, GatewayServiceStub

class GatewaySession:
    """A Gateway session to manage one stream for communication between Agents"""
    def __init__(self, session_id: str=None, stream_stream_call=None, timeout=1e9):
        self.session_id = session_id
        self.stream_stream_call = stream_stream_call
        self.timeout = timeout

    async def send(self, message):
        try:
            await self.stream_stream_call.write(message)
        except Exception as e:
            raise RuntimeError(f"Failed to send message: {e}")

    async def get_response(self):
        async for response in self.stream_stream_call:
            yield response

class GatewaySessionMagager:
    """Session manager of Gateway host"""

    def __init__(self):
        self.route_sessions: Dict[str, GatewaySession] = {}
        self._lock = asyncio.Lock()

    async def create_or_get_session(self, stub: AgentServiceStub, session_id: str) -> GatewaySession:
        """
        Create a new route session or return an existing one.
        Args:
            session_id (str): The original session ID.
        Returns:
            GatewaySession: The session object.
        """
        async with self._lock:
            if session := self.route_sessions.get(session_id):
                return session
            new_session = GatewaySession(session_id=session_id, stream_stream_call=stub.CallAgent())
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
                pass