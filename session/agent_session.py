import time
import asyncio
from typing import Dict, Callable
from grpc_service.type import AgentMessage, SessionStatus


class AgentClientSession:
    def __init__(self, stream_stream_call, timeout=1e9):
        self.stream_stream_call = stream_stream_call
        self.timeout = timeout
        self.session_id =  f"agent_session_{time.strftime('%Y%m%d_%H%M%S', time.localtime())}"

        self.active_session = None
        self.response_queue = None
        self._receive_task = None
        self._running = False

    async def _handle_response(self):
        try:
            async for response in self.stream_stream_call:
                _response = AgentMessage.from_grpc(response)
                await self.response_queue.put(_response)

                if _response.session_status == SessionStatus.STOP_RESPONSE:
                    if self.active_session and not self.active_session.done():
                        self.active_session.set_result(_response)
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

    async def create_session(self):
        self.active_session = asyncio.Future()
        self.response_queue = asyncio.Queue()
        self._running = True

        if self._receive_task is None or self._receive_task.done():
            self._receive_task = asyncio.create_task(self._handle_response())

        return self

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
        self._is_active = True
        self._processor_task = asyncio.create_task(self._process_requests())

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
                await self.response_queue.put(response)

                # flag done
                self.request_queue.task_done()

        except asyncio.CancelledError:
            pass
        finally:
            pass

    async def put_request(self, message: AgentMessage):
        if self._is_active:
            await self.request_queue.put(message)

    async def get_response(self):
        while self._is_active or not self.response_queue.empty():
            yield await self.response_queue.get()

    async def close(self):
        self._is_active = False
        self._processor_task.cancel()
        try:
            await self._processor_task
        except asyncio.CancelledError:
            pass


class AgentServerSessionManager:
    """session manager of Agent server"""

    def __init__(self):
        self.active_sessions: Dict[str, AgentServerSession] = {}
        self._lock = asyncio.Lock()

    async def create_or_get_session(self, session_id: str, process_request_func: Callable) -> AgentServerSession:
        """
        Create a new session or return an existing one.
        Args:
            session_id (str): The session ID.
            process_request_func (Callable): The function to process requests.
        Returns:
            AgentServerSession: The session object.
        """
        async with self._lock:
            if session := self.active_sessions.get(session_id):
                return session
            new_session = AgentServerSession(session_id, process_request_func)
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