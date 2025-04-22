import time
import asyncio
from grpc_service.type import AgentMessage, SessionStatus


class AgentClientSession:
    def __init__(self, stream_stream_call, timeout=1e9):
        self.stream_stream_call = stream_stream_call
        self.timeout = timeout

        self.response_queue = asyncio.Queue()  # 新增响应队列
        self.session_id =  f"agent_session_{time.strftime('%Y%m%d_%H%M%S', time.localtime())}"
        self.active_session = None
        self._receive_task = None
        self._running = False

    async def _handle_response(self):
        try:
            async for response in self.stream_stream_call:
                _response = AgentMessage.from_grpc(response)
                await self.response_queue.put(_response)  # 所有响应入队

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
            del self.active_session
        self.response_queue = asyncio.Queue()

    async def create_session(self):
        self.active_session = asyncio.Future()
        self._running = True

        if self._receive_task is None or self._receive_task.done():
            self._receive_task = asyncio.create_task(self._handle_response())

        return self  # 返回自身以支持异步迭代

    async def stream_responses(self):
        """异步迭代器，持续获取响应"""
        while self._running or not self.response_queue.empty():
            try:
                yield await asyncio.wait_for(self.response_queue.get(), timeout=0.1)
            except asyncio.TimeoutError:
                if not self._running:
                    break

    async def wait_for_stop(self):
        """等待最终停止响应（兼容原有逻辑）"""
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
