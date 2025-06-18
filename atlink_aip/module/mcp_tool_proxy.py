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
        self.read_queue = asyncio.Queue()
        self.write_queue = asyncio.Queue()

    async def _connect_to_mcp_server(self, headers=None, auth=None, timeout=300, sse_read_timeout=300):
        endpoint_future = asyncio.Future()
        timeout_obj = aiohttp.ClientTimeout(total=timeout, sock_read=sse_read_timeout)
        session = aiohttp.ClientSession(headers=headers, auth=auth, timeout=timeout_obj)

        self.reader_task = asyncio.create_task(sse_reader(self.mcp_url, session, timeout_obj, endpoint_future, self.read_queue))
        self.writer_task = asyncio.create_task(post_writer(session, endpoint_future, self.write_queue))
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
        while not self.read_queue.empty():
            self.read_queue.get_nowait()
        while not self.write_queue.empty():
            self.write_queue.get_nowait()

    async def _initialize_mcp(self) -> Any:
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
        try:
            response = await asyncio.wait_for(self.read_queue.get(), timeout=self.timeout)
            return json.loads(response)
        except asyncio.TimeoutError:
            self._logger.error("[MCP] Initializing Failed: timeout error")
            return None
        except Exception as e:
            self._logger.error(f"[MCP] Initializing Failed: {str(e)}")
            return None
        finally:
            json_content = {
                "method": "notifications/initialized",
                "jsonrpc": "2.0"
            }
            await self.write_queue.put(json_content)

    async def _list_mcp_tools(self) -> dict | None:
        json_content = {
            "method": "tools/list",
            "jsonrpc": "2.0",
            "id": 0
        }

        await self.write_queue.put(json_content)
        try:
            response = await asyncio.wait_for(self.read_queue.get(), timeout=self.timeout)
            return json.loads(response)
        except asyncio.TimeoutError:
            self._logger.error("[MCP] List mcp tools failed: timeout error")
            return None
        except Exception as e:
            self._logger.error(f"[MCP] List mcp tools failed:: {str(e)}")
            return None

    async def _call_mcp_tool(self, name, arguments, id):
        json_content = {
            "method": "tools/call",
            "params": {
                "name": name,
                "arguments": arguments
            },
            "jsonrpc": "2.0",
            "id": id
        }

        await self.write_queue.put(json_content)
        try:
            response = await asyncio.wait_for(self.read_queue.get(), timeout=self.timeout)
            return json.loads(response)
        except asyncio.TimeoutError:
            self._logger.error("[MCP] Call tool failed: timeout error")
            return None
        except Exception as e:
            self._logger.error(f"[MCP] Call tool failed:: {str(e)}")
            return None

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
            await self._initialize_mcp()

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

        resp = await self._initialize_mcp()
        if resp:
            self.name = resp['result']['serverInfo']['name']
            self.description += f" MCP version: {resp['result']['protocolVersion']}"

        resp = await self._list_mcp_tools()
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