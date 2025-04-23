import time
import asyncio
from typing import Optional
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
