# -*- coding: utf-8 -*-
"""
Created on Wed Apr 23 12:00:00 2025

@author: haixinwa
"""
import time
import uuid
import asyncio
from typing import Optional, Callable
from grpc_service.type import ToolRequest, ToolResponse


class ToolClientSession:
    def __init__(self, unary_unary_call, timeout=1e9):
        self.unary_unary_call = unary_unary_call
        self.timeout = timeout

        self.session_id =  None
        self.active_session = None
        self._running = False

    def _cleanup(self):
        if self.active_session:
            self.active_session.cancel()
        self._running = False
        self.active_session = None

    async def activate(self):
        _time = str(time.strftime('%Y%m%d_%H%M%S', time.localtime()))
        _uuid = str(uuid.uuid4())
        session_id = f"tool_session_{_uuid}_{_time}"
        self.session_id = session_id
        self.active_session = asyncio.Future()
        self._running = True

        return self

    async def send(self, message: ToolRequest)-> Optional[ToolResponse]:
        if not self.active_session:
            raise RuntimeError("Session not created")

        try:
            response =  await asyncio.wait_for(self.unary_unary_call(message.to_grpc()), timeout=self.timeout)
            _response = ToolResponse.from_grpc(response)

            return _response

        except asyncio.TimeoutError as e:
            if not self.active_session.done():
                self.active_session.set_exception(e)
            raise TimeoutError(f"Session {self.session_id} timed out") from e

        except Exception as e:
            if not self.active_session.done():
                self.active_session.set_exception(e)
            raise

    async def close(self):
        self._running = False
        if self.active_session and not self.active_session.done():
            self.active_session.set_result('done')
            self.active_session.cancel()

        self._cleanup()


class ToolServerSession:
    """A single session instance on the server side"""

    def __init__(self, session_id: str, process_request_func: Callable):
        self.session_id = session_id
        self.process_request_func = process_request_func

    async def process_request(self, request: ToolRequest) -> ToolResponse:
        response = await self.process_request_func(ToolRequest.from_grpc(request))
        response.session_id = self.session_id

        return response