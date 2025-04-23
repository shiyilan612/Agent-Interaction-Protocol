import time
import asyncio
from typing import Dict, Callable, Optional
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
        self.session_id = f"tool_session_{time.strftime('%Y%m%d_%H%M%S', time.localtime())}"
        self.active_session = asyncio.Future()
        self._running = True

        return self

    async def send(self, message: ToolRequest)-> Optional[ToolResponse]:
        if not self.active_session:
            raise RuntimeError("Session not created")

        try:
            response =  await asyncio.wait_for(self.unary_unary_call(message.to_grpc()), timeout=self.timeout)
            _response = ToolResponse.from_grpc(response)
            self.active_session.set_result('done')

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
            self.active_session.cancel()

        self._cleanup()


class ToolServerSession:
    """A single session instance on the server side"""

    def __init__(self, session_id: str, process_request_func: Callable):
        self.session_id = session_id
        self.process_request_func = process_request_func
        self._processor_task = None

    async def activate(self):
        self._processor_task = asyncio.create_task(self.process_request_func)
        return self

    async def close(self):
        if self._processor_task:
            self._processor_task.cancel()


class ToolServerSessionManager:
    """session manager of Agent server"""

    def __init__(self):
        self.active_sessions: Dict[str, ToolServerSession] = {}
        self._lock = asyncio.Lock()

    async def create_or_get_session(self, session_id: str, process_request_func: Callable) -> ToolServerSession:
        async with self._lock:
            if session := self.active_sessions.get(session_id):
                return session
            new_session = ToolServerSession(session_id, process_request_func)
            asyncio.create_task(new_session.activate())
            self.active_sessions[session_id] = new_session

            return new_session

    async def close_session(self, session_id: str):
        async with self._lock:
            if session := self.active_sessions.pop(session_id, None):
                await session.close()