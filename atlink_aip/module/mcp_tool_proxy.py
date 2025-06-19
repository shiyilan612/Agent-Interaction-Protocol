# -*- coding: utf-8 -*-
"""
Created on Thu Apr 24 23:39:08 2025

@author: haixinwa
"""
# -*- coding: utf-8 -*-

import uuid
import json
import aiohttp
import asyncio
from typing import Any, Callable, List
from .toolbox import ToolBox
from .base import sse_reader, post_writer
from .server import ToolServer
from ..grpc_service.type import ToolInfo, ToolBoxInfo, ToolRequest, ToolResponse, ContentItem


class MCPToolProxy(ToolBox):
    def __init__(self,
                 mcp_url: str,
                 address: str,
                 toolbox_id: str = None,
                 domain: str = "default",
                 description: str = ""):
        """
        Initialize a new ToolBox.

        Args:
            mcp_url: Address of mcp server
            address: Address where this proxy will be hosted (e.g., "localhost:50051")
            toolbox_id: Unique identifier for this MCPTool (defaults to UUID if not provided)
            domain: MCPTool group/domain
            description: Detailed description of the MCPTool
        """
        super().__init__(
            address=address,
            name="",
            toolbox_id=toolbox_id if toolbox_id else f"mcptool_{str(uuid.uuid4())}",
            domain=domain,
            description=description
        )

        self.mcp_url = mcp_url
        self.pending_queue = {}
        self.write_queue = asyncio.Queue()

    async def _connect_to_mcp_server(self, headers=None, auth=None, timeout=300, sse_read_timeout=300):
        endpoint_future = asyncio.Future()
        timeout_obj = aiohttp.ClientTimeout(total=timeout, sock_read=sse_read_timeout)
        session = aiohttp.ClientSession(headers=headers, auth=auth, timeout=timeout_obj)

        self.reader_task = asyncio.create_task(
            sse_reader(self.mcp_url, session, timeout_obj, endpoint_future, self.pending_queue, self._logger)
        )
        self.writer_task = asyncio.create_task(
            post_writer(session, endpoint_future, self.write_queue, self._logger)
        )
        self.mcp_session = session
        self.timeout = timeout

    async def _close_mcp_connection(self):
        # Cancel the task.
        for task in [self.reader_task, self.writer_task]:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            except Exception as e:
                self._logger.error(f"Task cleanup error: {str(e)}")

        # close session
        await self.mcp_session.close()

        # clear queue
        for read_queue in self.pending_queue:
            while not read_queue.empty():
                read_queue.get_nowait()
        while not self.write_queue.empty():
            self.write_queue.get_nowait()

    async def _invoke_mcp(self, json_content:dict, id:str, flag:str):
        read_queue = asyncio.Queue()
        self.pending_queue[id] = read_queue

        await self.write_queue.put(json_content)

        try:
            response = await asyncio.wait_for(read_queue.get(), timeout=self.timeout)
            return response
        except asyncio.TimeoutError:
            self._logger.error(f"[MCP] {flag} Failed: timeout error")
            return None
        except Exception as e:
            self._logger.error(f"[MCP] {flag} Failed: {str(e)}")
            return None
        finally:
            del self.pending_queue[id]  # 清理

    async def _initialize_mcp(self, id) -> Any:
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
            "id": id
        }

        response = await self._invoke_mcp(json_content, id, flag="Initialize")
        json_content = {
            "method": "notifications/initialized",
            "jsonrpc": "2.0"
        }
        await self.write_queue.put(json_content)

        return response

    async def _list_mcp_tools(self, id) -> dict | None:
        json_content = {
            "method": "tools/list",
            "jsonrpc": "2.0",
            "id": id
        }

        response = await self._invoke_mcp(json_content, id, flag="List Tools")

        return response

    async def _call_mcp_tool(self, name, arguments, id) -> dict | None:
        json_content = {
            "method": "tools/call",
            "params": {
                "name": name,
                "arguments": arguments
            },
            "jsonrpc": "2.0",
            "id": id
        }

        response = await self._invoke_mcp(json_content, id, flag="Call Tool")

        return response

    def _update_toolbox_info(self, tools_info:List=[]) -> None:
        """Update the toolbox information when tools change."""
        self.tools_info = tools_info
        self.toolbox_info = ToolBoxInfo(
            toolbox_id=self.toolbox_id,
            address=self.address,
            name=self.name,
            domain=self.domain,
            description=self.description,
            tools=self.tools_info
        )
        if self._server:
            updata_task = asyncio.create_task(self._server.update_toolbox_info(self.toolbox_info.to_grpc()))
            def handle_update_exception(task: asyncio.Task):
                try:
                    task.result()
                except Exception as e:
                    self._logger.error(f"Failed to update ToolBox information with Gateway - Exception: {str(e)}")
            updata_task.add_done_callback(handle_update_exception)

    async def _call_tool_handler(self, request: ToolRequest) -> ToolResponse:
        """
        Handle incoming tool requests by invoking the appropriate tool

        Args:
            request: The incoming tool request

        Returns:
            ToolResponse containing the result or error
        """
        self._logger.info(f"<ToolBox>: ToolBox [{self.toolbox_id}] received request from "
                          f"[{request.sender_id}]")
        assert (request.receiver_id == self.toolbox_id)


        try:
            await self._connect_to_mcp_server()
            await self._initialize_mcp(id=request.session_id)

            mcp_resp = await self._call_mcp_tool(
                name=request.tool_name,
                arguments=json.loads(request.arguments),
                id=request.session_id
            )

            citem_list = list()
            for c in mcp_resp['result']['content']:
                citem = ContentItem.write_text(str(c['text']))
                citem_list.append(citem)

            response = ToolResponse(
                sender_id=self.toolbox_id,
                receiver_id=request.sender_id,
                session_id=request.session_id,
                content=citem_list,
                is_error=False,
                error_message="No Error"
            )
            return response

        except Exception as e:
            response = ToolResponse(
                sender_id=self.toolbox_id,
                receiver_id=request.sender_id,
                session_id=request.session_id,
                content=[],
                is_error=True,
                error_message=f"Error in calling MCP tool: {str(e)}"
            )

            return response

        finally:
            await self._close_mcp_connection()

    async def start(self):
        # Start the tool server.
        self._server = ToolServer(self.toolbox_info, self._call_tool_handler)
        await self._server.start()

        # Connect to MCP Server
        await self._connect_to_mcp_server()

        id = str(uuid.uuid4())
        resp = await self._initialize_mcp(id)
        if resp:
            self.name = resp['result']['serverInfo']['name']
            self.description += f" MCP version: {resp['result']['protocolVersion']}"

        resp = await self._list_mcp_tools(id)
        if resp:
            tools_info = list()
            for t in resp['result']['tools']:
                tools_info.append(
                    ToolInfo(
                        name=t['name'],
                        description=t['description'],
                        arguments=str(t['inputSchema']['properties'])
                    )
                )
            self._update_toolbox_info(tools_info)

        await self._close_mcp_connection()

        return self

    async def stop(self):
        # Stop the tool server.
        if self._server:
            await self._server.stop()

        # Stop loggers
        self._logger_mgr.stop()

        # Close MCP connection
        await self._close_mcp_connection()

    def add_tool(
        self,
        fn: Callable,
        name: str | None = None,
        description: str | None = None,
        version: str | None = None
    ):
        pass

    def tool(
        self,
        name: str | None = None,
        description: str | None = None,
        version: str | None = None
    ):
        pass