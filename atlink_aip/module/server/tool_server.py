# -*- coding: utf-8 -*-
"""
Created on Wed Apr 23 12:00:00 2025

@author: haixinwa & xmkang
"""

# -*- coding: utf-8 -*-
import grpc
from typing import Callable
from ...grpc_service import ToolService
from ...grpc_service.type import ToolRequest, ToolBoxInfo
from ...session import ToolServerSession


class ToolServer(ToolService):
    def __init__(self, toolbox_info: ToolBoxInfo, process_request_func: Callable):
        super().__init__(toolbox_info.to_grpc())
        self.process_request_func = process_request_func

    async def CallTool(self, request, context):
        try:
            request = ToolRequest.from_grpc(request)
            session_id = request.session_id
            
            sender_id = request.sender_id
            receiver_id = request.receiver_id
            self._logger.info(f"<Tool>: [ToolRequest {sender_id} -> {receiver_id}] "
                               f"Request received")

            session = ToolServerSession(session_id, self.process_request_func)
            response = await session.process_request(request)
            
            self._logger.info(f"<Tool>: [ToolResponse {receiver_id} -> {sender_id}] "
                               f"Response sent")

            return response.to_grpc()

        except Exception as e:
            await context.abort(
                code=grpc.StatusCode.INTERNAL,
                details=f"Handling request failure: {str(e)}"
            )