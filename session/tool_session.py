import time
import asyncio
from typing import Dict
from grpc_service.schema_pb2 import ToolRequest, ToolResponse


class ToolSession:
    def __init__(self):
        self.active_requests: Dict[str, asyncio.Future] = {}

    def generate_tool_session_id(self, tool_id) -> str:
        return f"{tool_id}_{time.strftime('%Y%m%d_%H%M%S', time.localtime())}"

    async def create_session(self, request: ToolRequest) -> ToolResponse:
        pass
