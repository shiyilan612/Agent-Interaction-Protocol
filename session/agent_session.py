# -*- coding: utf-8 -*-
"""
Created on Mon Apr 21 12:00:00 2025

@author: haixinwa
"""
import uuid
import time
import asyncio
import grpc
from typing import Dict, Callable
from grpc_service.type import AgentMessage, SessionStatus


class AgentClientSession:
    def __init__(self, stream_stream_call, session_id=None, timeout=1e9):
        self.stream_stream_call = stream_stream_call
        self.timeout = timeout

        self.session_id =  session_id
        self.active_session = None
        self.response_queue = None
        self._receive_task = None
        self._running = False

    async def _handle_response(self):
        try:
            async for _response in self.stream_stream_call:
                response = AgentMessage.from_grpc(_response)
                await self.response_queue.put(response)

                if response.session_status == SessionStatus.STOP_RESPONSE:
                    if self.active_session and not self.active_session.done():
                        self.active_session.set_result(response)
        except grpc.RpcError as rpc_error:
            # TODO: 加入异常处理log逻辑，统一异常处理模块？
            if self.active_session and not self.active_session.done():
                self.active_session.set_exception(rpc_error)
            self._running = False
            raise
        except Exception as e:
            if self.active_session and not self.active_session.done():
                self.active_session.set_exception(e)
            self._running = False
            raise

    def _cleanup(self):
        if self.active_session:
            self.active_session.cancel()
        del self.response_queue
        self._running = False

    async def activate(self):
        if not self.session_id:
            _time = str(time.strftime('%Y%m%d_%H%M%S', time.localtime()))
            _uuid = str(uuid.uuid4())
            self.session_id = f"agent_session_{_uuid}_{_time}"
        self.active_session = asyncio.Future()
        self.response_queue = asyncio.Queue()
        self._running = True

        if self._receive_task is None or self._receive_task.done():
            self._receive_task = asyncio.create_task(self._handle_response())

        return self.session_id

    async def stream_responses(self):
        """obtain continuous response"""
        while self._running or not self.response_queue.empty():
            try:
                yield await asyncio.wait_for(self.response_queue.get(), timeout=0.1)
            except asyncio.TimeoutError:
                if not self._running:
                    break

    async def wait_for_stop(self):
        """wait for the final stop response"""
        try:
            return await asyncio.wait_for(self.active_session, self.timeout)
        except asyncio.TimeoutError:
            self.active_session.cancel()
            raise TimeoutError(f"Session {self.session_id} timed out")
        finally:
            self._cleanup()

    async def send(self, message):
        if not self.active_session:
            raise RuntimeError("Session not created")
        await self.stream_stream_call.write(message.to_grpc())

    async def close(self):
        self._running = False
        if self._receive_task:
            self._receive_task.cancel()
            try:
                await self._receive_task
            except asyncio.CancelledError:
                pass
        if self.active_session and not self.active_session.done():
            self.active_session.cancel()
        self._cleanup()


class AgentServerSession:
    """A single session instance on the server side"""

    def __init__(self, session_id: str):
        """
        Args:
            session_id (str): The session ID.
        """
        self.session_id = session_id
        self.request_queue = None
        self.response_queue = None
        self._is_active = False

    async def activate(self):
        self._is_active = True
        self.request_queue = asyncio.Queue()
        self.response_queue = asyncio.Queue()

        return self

    async def put_request(self, message: AgentMessage):
        if self._is_active:
            await self.request_queue.put(message)

    async def get_request(self):
        while self._is_active or not self.request_queue.empty():
            request = await self.request_queue.get()
            yield request

    async def put_response(self, message: AgentMessage):
        if self._is_active:
            await self.response_queue.put(message)

    async def get_response(self):
        while self._is_active or not self.response_queue.empty():
            response = await self.response_queue.get()
            yield response

    async def close(self):
        self._is_active = False


class AgentServerSessionManager:
    """session manager of Agent server"""

    def __init__(self):
        self.active_sessions: Dict[str, AgentServerSession] = {}
        self._lock = asyncio.Lock()

    async def create_or_get_session(self, session_id: str) -> AgentServerSession:
        """
        Create a new session or return an existing one.
        Args:
            session_id (str): The session ID.
        Returns:
            AgentServerSession: The session object.
        """
        async with self._lock:
            if session := self.active_sessions.get(session_id):
                return session
            new_session = AgentServerSession(session_id)
            await new_session.activate()
            self.active_sessions[session_id] = new_session

            return new_session

    async def close_session(self, session_id: str):
        """
        Close a session by its ID.
        Args:
            session_id (str): The session ID.
        """
        async with self._lock:
            if session := self.active_sessions.pop(session_id, None):
                await session.close()