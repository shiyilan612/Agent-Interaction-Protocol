# -*- coding: utf-8 -*-
"""
Created on Mon Apr 21 12:00:00 2025

@author: haixinwa
"""
import uuid
import time
import asyncio
import logging
from typing import Dict, Callable
from grpc_service.type import AgentMessage, SessionStatus


class AgentClientSession:
    def __init__(self, stream_stream_call, timeout=1e9):
        self.stream_stream_call = stream_stream_call
        self.timeout = timeout

        self.session_id =  None
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
        except Exception as e:
            if self.active_session and not self.active_session.done():
                self.active_session.set_exception(e)
            self._running = False
            raise

    def _cleanup(self):
        if self.active_session:
            self.active_session.cancel()
        self.response_queue = asyncio.Queue()
        self._running = False

    async def activate(self):
        _time = str(time.strftime('%Y%m%d_%H%M%S', time.localtime()))
        _uuid = str(uuid.uuid4())
        session_id = f"agent_session_{_uuid}_{_time}"
        self.session_id = session_id
        self.active_session = asyncio.Future()
        self.response_queue = asyncio.Queue()
        self._running = True

        if self._receive_task is None or self._receive_task.done():
            self._receive_task = asyncio.create_task(self._handle_response())

        return session_id

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

    def __init__(self, session_id: str, process_request_func: Callable):
        """
        Args:
            session_id (str): The session ID.
            process_request_func (Callable): The function to process requests.
        """
        self.session_id = session_id
        self.process_request_func = process_request_func
        self.request_queue = asyncio.Queue()
        self.response_queue = asyncio.Queue()
        self._is_active = False
        self._processor_task = None

    async def _process_requests(self):
        """asynchronous processing of requests"""
        try:
            while self._is_active or not self.request_queue.empty():
                # get request
                try:
                    request = await asyncio.wait_for(
                        self.request_queue.get(),
                        timeout=1.0
                    )
                except asyncio.TimeoutError:
                    continue

                response = await self.process_request_func(request)

                if response.session_status not in [SessionStatus.HOLD_RESPONSE, SessionStatus.STOP_RESPONSE]:
                    raise RuntimeError(f"Invalid session status in response: {response.session_status}")
                    
                await self.response_queue.put(response)

                # flag done
                self.request_queue.task_done()
        except RuntimeError as e:
            await self.response_queue.put(e)
            raise
        except asyncio.CancelledError:
            pass
        finally:
            pass

    async def activate(self):
        self._is_active = True
        self._processor_task = asyncio.create_task(self._process_requests())

        return self

    async def put_request(self, message: AgentMessage):
        if self._is_active:
            await self.request_queue.put(message)

    async def get_response(self):
        while self._is_active or not self.response_queue.empty():
            response = await self.response_queue.get()
            if isinstance(response, Exception):
                self.close()
                raise response
            yield response

    async def close(self):
        self._is_active = False
        if self._processor_task:
            self._processor_task.cancel()


class AgentServerSessionManager:
    """session manager of Agent server"""

    def __init__(self, logger: logging.Logger):
        self.active_sessions: Dict[str, AgentServerSession] = {}
        self._lock = asyncio.Lock()
        self._logger = logger

    async def create_or_get_session(self, 
                                    session_id: str, 
                                    client_id: str,
                                    process_request_func: Callable) -> AgentServerSession:
        """
        Create a new session or return an existing one.
        Args:
            session_id (str): The session ID.
            client_id (str): The client ID.
            process_request_func (Callable): The function to process requests.
        Returns:
            AgentServerSession: The session object.
        """
        async with self._lock:
            if session := self.active_sessions.get(session_id):
                return session
            new_session = AgentServerSession(session_id, process_request_func)
            await new_session.activate()
            self.active_sessions[session_id] = new_session

            self._logger.debug(f"<Agent>: Session [{session_id}] created for processing requests of"
                              f" Agent [{client_id}]")
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