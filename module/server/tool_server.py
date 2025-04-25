# -*- coding: utf-8 -*-
"""
Created on Wed Apr 23 12:00:00 2025

@author: haixinwa
"""

# -*- coding: utf-8 -*-
import grpc
from typing import Callable
from grpc_service import ToolService
from grpc_service.type import ToolRequest, ToolInfo
from session import ToolServerSession


class ToolServer(ToolService):
    def __init__(self, tool_info: ToolInfo, process_request_func: Callable):
        super().__init__(tool_info.to_grpc())
        self.process_request_func = process_request_func

    async def CallTool(self, request, context):
        try:
            request = ToolRequest.from_grpc(request)
            session_id = request.session_id

            session = ToolServerSession(session_id, self.process_request_func)
            response = await session.process_request(request)

            return response.to_grpc()

        except Exception as e:
            await context.abort(
                code=grpc.StatusCode.INTERNAL,
                details=f"Handling request failure: {str(e)}"
            )
