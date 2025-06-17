# -*- coding: utf-8 -*-
"""
Created on Thu Apr 24 23:39:08 2025

@author: haixinwa
"""
from os import eventfd
from xmlrpc.client import Boolean

# -*- coding: utf-8 -*-
import grpc
import json
import logging
import aiohttp
import asyncio
from .sse_sesion import sse_reader, post_writer
from ...grpc_service import ToolService
from ...grpc_service.type import ToolInfo
from ...grpc_service import schema_pb2 as pb2


logger = logging.getLogger(__name__)


class MCPToolProxy(ToolService):
    def __init__(self, tool_info: ToolInfo):
        super().__init__(tool_info.to_grpc())
        self.reader_task = None
        self.writer_task = None
        self.session = None
        self.read_queue = asyncio.Queue()
        self.write_queue = asyncio.Queue()

    async def connect_to_mcp_server(self, mcp_url:str, headers=None, auth=None, timeout=30, sse_read_timeout=300):
        endpoint_future = asyncio.Future()
        timeout_obj = aiohttp.ClientTimeout(total=timeout, sock_read=sse_read_timeout)
        session = aiohttp.ClientSession(headers=headers, auth=auth,timeout=timeout_obj)

        self.reader_task = asyncio.create_task(sse_reader(mcp_url, session, timeout_obj, endpoint_future, self.read_queue))
        self.writer_task = asyncio.create_task(post_writer(session, endpoint_future, self.write_queue))
        self.session = session

    async def close_mcp_connection(self):
        # Cancel the task.
        for task in [self.reader_task, self.writer_task]:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.debug(f"Task cleanup error: {str(e)}")

        # close session
        await self.session.close()

        # clear queue
        while not self.read_queue.empty():
            self.read_queue.get_nowait()
        while not self.write_queue.empty():
            self.write_queue.get_nowait()

    async def initialize_mcp(self) -> bool:
        json_content = {
            "method": "initialize",
            "params": {
                "protocolVersion": "2025-03-26",
                "capabilities": {},
                "clientInfo": {
                    "name": "mcp",
                    "version": "0.1.0"
                }
            },
            "jsonrpc": "2.0",
            "id": 0
        }

        await self.write_queue.put(json_content)
        result = await self.read_queue.get()
        if result:
            logger.info(result)
            json_content = {
                "method": "notifications/initialized",
                "jsonrpc": "2.0"
            }
            await self.write_queue.put(json_content)
        else:
            logger.error("Initializing Failed.")

    async def list_mcp_tools(self) -> dict:
        json_content = {
            "method": "tools/list",
            "jsonrpc": "2.0",
            "id": 0
        }

        await self.write_queue.put(json_content)
        result = await self.read_queue.get()

        return json.loads(result)

    async def call_mcp_tool(self, name, arguments):
        json_content = {
            "method": "tools/call",
            "params": {
                "name": name,
                "arguments": arguments
            },
            "jsonrpc": "2.0",
            "id": 0
        }

        await self.write_queue.put(json_content)
        result = await self.read_queue.get()

        return json.loads(result)

    async def CallTool(self, request: pb2.ToolRequest, context) -> pb2.ToolResponse:
        """Main gRPC endpoint handling"""
        try:
            # Convert gRPC -> JSON-RPC
            jsonrpc_request = await self._convert_grpc_to_jsonrpc(request)

            # Forward to JSON-RPC server
            json_response = await self._send_jsonrpc_request(jsonrpc_request)

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