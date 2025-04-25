# -*- coding: utf-8 -*-
"""
Created on Thu Apr 24 23:39:08 2025

@author: haixinwa
"""

# -*- coding: utf-8 -*-
import grpc
import aiohttp
from grpc_service import ToolService
from grpc_service.type import ToolInfo
from grpc_service import schema_pb2 as pb2


class MCPToolServerProxy(ToolService):
    def __init__(self, tool_info: ToolInfo, jsonrpc_server_url):
        super().__init__(tool_info.to_grpc())
        self.jsonrpc_server_url= jsonrpc_server_url

    async def _convert_grpc_to_jsonrpc(self, grpc_request: pb2.ToolRequest) -> dict:
        """Convert gRPC Request to JSON-RPC format"""
        json_request = {
            "jsonrpc": "2.0",
            "method": "tools/call",
            "params": {
                "name": grpc_request.tool_name,
                "arguments": grpc_request.arguments
            },
            "id": grpc_request.session_id
        }

        return json_request

    async def _convert_jsonrpc_to_grpc(self, json_response: dict) -> pb2.ToolResponse:
        """Convert JSON-RPC Response to gRPC format"""

        return pb2.ToolResponse(
            session_id=json_response["id"],
            is_error=json_response["isError"],
            # content=[ParseDict(item, pb2.ContentItem) for item in json_response["content"]],
        )


    async def CallTool(self, request: pb2.ToolRequest, context) -> pb2.ToolResponse:
        """Main gRPC endpoint handling"""
        try:
            # Convert gRPC -> JSON-RPC
            jsonrpc_request = await self._convert_grpc_to_jsonrpc(request)

            # Forward to JSON-RPC server
            async with aiohttp.ClientSession() as session:
                async with session.post(
                        self.jsonrpc_server_url,
                        json=jsonrpc_request,
                        headers={"Content-Type": "application/json"}
                ) as resp:
                    json_response = await resp.json()

            # Convert JSON-RPC -> gRPC
            grpc_response = await self._convert_jsonrpc_to_grpc(json_response)
            grpc_response.sender_id = request.receiver_id
            grpc_response.receiver_id = request.sender_id

            return grpc_response

        except Exception as e:
            await context.abort(
                code=grpc.StatusCode.INTERNAL,
                details=f"Handling request failure: {str(e)}"
            )