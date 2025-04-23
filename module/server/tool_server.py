import grpc
from typing import Callable
from grpc_service import ToolService
from grpc_service.type import ToolRequest, ToolInfo


class ToolServer(ToolService):
    def __init__(self, tool_info: ToolInfo, process_request_func: Callable):
        super().__init__(tool_info.to_grpc())
        self.process_request_func = process_request_func

    async def CallTool(self, request, context):
        try:
            response = await self.process_request_func(ToolRequest.from_grpc(request))
            return response.to_grpc()
        except Exception as e:
            await context.abort(
                code=grpc.StatusCode.INTERNAL,
                details=f"Handling request failure: {str(e)}"
            )
