from typing import Callable
from grpc_service import ToolService
from grpc_service.type import ToolRequest, ToolInfo
from session import ToolServerSessionManager


class ToolServer(ToolService):
    def __init__(self, tool_info: ToolInfo, process_request_func: Callable):
        super().__init__(tool_info.to_grpc())
        self.session_mgr = ToolServerSessionManager()
        self.process_request_func = process_request_func

    async def CallTool(self, request, context):
        try:
            _request = ToolRequest.from_grpc(request)
            client_session_id = _request.session_id
            session = await self.session_mgr.create_or_get_session(client_session_id, self.process_request_func)

        finally:
            await self.session_mgr.close_session(client_session_id)