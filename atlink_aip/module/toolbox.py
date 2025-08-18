# -*- coding: utf-8 -*-
"""
Created on Tue Jun 17 9:36:12 2025

@author: clleng, haixinwa
"""

import uuid
import asyncio
from typing import Callable, List
from .base import Tool
from .server import ToolServer
from ..grpc_service.type import ToolInfo, ToolBoxInfo, ToolRequest, ToolResponse
from ..logger import LoggerManager


class ToolBox:
    """
    A container for multiple tools that can be registered to a gateway and used to process requests.

    The ToolBox acts as a high-level interface for managing available tools, handling incoming 
    requests, and communicating with the gateway service. It wraps the underlying ToolServer 
    implementation and provides a unified way to register, organize, and invoke tools.
    """
    
    def __init__(self, 
                 host_address: str,
                 service_address: str = None,
                 name: str = None,
                 toolbox_id: str = None,
                 domain: str = "default",
                 description: str = "",
                 warn_on_duplicate_tools: bool = True,
                 tools: List[Tool] | None = None):
        """
        Initialize a new ToolBox.
        
        Args:
            host_address: Address where this toolbox will be hosted (e.g., "localhost:50051")
            service_address: Address of the service provided by the toolbox to the outside
            toolbox_id: Unique identifier for this toolbox (defaults to UUID if not provided)
            name: Human-readable name for this toolbox
            domain: toolbox group/domain 
            description: Detailed description of the toolbox
            warn_on_duplicate_tools: Whether to log warnings for duplicate tool
            tools: Optional list of initial tools to register in this toolbox
        """
        self.host_address = host_address
        self.service_address = service_address if service_address else host_address
        self.toolbox_id = toolbox_id if toolbox_id else f"toolbox_{str(uuid.uuid4())}"
        self.name = name if name else self.toolbox_id
        self.domain = domain
        self.description = description

        # Logger setup
        self._logger_mgr = LoggerManager()
        self._logger = self._logger_mgr.get_logger(self.toolbox_id)
        
        # Tool properties
        self._tools: dict[str, Tool] = {}
        if tools is not None:
            for tool in tools:
                if warn_on_duplicate_tools and tool.name in self._tools:
                    self._logger.warning(f"<ToolBox>: Tool already exists: {tool.name}")
                self._tools[tool.name] = tool

        self.warn_on_duplicate_tools = warn_on_duplicate_tools
        
        # Server instance
        self._server = None
        
        # Initialize toolbox information
        self._update_toolbox_info()
        
        # Gateway connection
        self._gateway_address = None

    async def _update_tool_info(self,
                               name=None,
                               description=None,
                               version=None,
                               arguments=None):
        """Update tool information"""

        tool = self._tools.get(self.name)
        if not tool:
            self._logger.error(f"<ToolBox>: Tool [{self.name}] not found in toolbox.")
        else:
            await tool.update_tool_info(name, description, arguments, version)
            self._update_toolbox_info()

        return self

    def _update_toolbox_info(self) -> None:
        """Update the toolbox information when tools change."""
        self.tools_info = [tool.tool_info for tool in self._tools.values()]
        self.toolbox_info = ToolBoxInfo(
            toolbox_id=self.toolbox_id,
            address=self.service_address,
            name=self.name,
            domain=self.domain,
            description=self.description,
            tools=self.tools_info
        )

       
        if self._server and hasattr(self._server, 'gateway_stub') and self._server.gateway_stub:
            updata_task = asyncio.create_task(self._server.update_toolbox_info(self.toolbox_info.to_grpc()))
            
            def handle_update_exception(task: asyncio.Task):
                try:
                    task.result()
                except Exception as e:
                    self._logger.error(f"Failed to update ToolBox information with Gateway - Exception: {str(e)}")
            
            updata_task.add_done_callback(handle_update_exception)
        else:
            self._logger.debug("Skipping toolbox info update: not connected to gateway")

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
        
        response = await self._tools[request.tool_name].invoke_tool(request)
        response.sender_id = self.toolbox_id

        return response
    
    async def start(self):
        """Start the tool server."""
        self._server = ToolServer(self.toolbox_info, self._call_tool_handler)
        await self._server.start()
        return self

    async def stop(self):
        """Stop the tool server."""
        if self._server:
            await self._server.stop()

        # Stop loggers
        self._logger_mgr.stop()

    async def register_to_gateway(self, gateway_address: str, timeout: float = 5.0):
        """Register to the gateway service.
        
        Args:
            gateway_address: Address of the gateway service (e.g., "localhost:50052")
            timeout: Timeout for registering to the gateway service (default is 5 seconds)
        """
        self._gateway_address = gateway_address
        if not self._server:
            raise RuntimeError("ToolBox server not started. Call start() first.")
            
        try:
            self._logger.info(f"<ToolBox>: Connecting to gateway at [{gateway_address}]...")
            await asyncio.wait_for(self._server.connect_to_gateway(gateway_address), timeout=timeout)
        except asyncio.TimeoutError:
            self._logger.error(f"<ToolBox>: Failed to register to Gateway at [{gateway_address}] - Timeout")
            raise
            
        return self
    
    def add_tool(
        self,
        fn: Callable,
        name: str | None = None,
        description: str | None = None,
        version: str | None = None
    ) -> None:
        """Add a tool to the server.

        Args:
            fn: The function to register as a tool
            name: Optional name for the tool (defaults to function name)
            description: Optional description of what the tool does
            version: Tool version
        """
        
        tool = Tool(fn, name=name, description=description, version=version)
        existing = self._tools.get(tool.name)
        if existing:
            if self.warn_on_duplicate_tools:
                self._logger.warning(f"<ToolBox>: Tool already exists: {tool.name}")
            return existing
        self._tools[tool.name] = tool
        self._update_toolbox_info()

        return tool
    
    def tool(
        self,
        name: str | None = None,
        description: str | None = None,
        version: str | None = None
    ) -> Callable[[Callable], Callable]:
        """Decorator to register a tool.

        Args:
            name: Optional name for the tool (defaults to function name)
            description: Optional description of what the tool does
            version: Tool version
        Example:
            @toolbox.tool()
            def my_tool(x: int) -> str:
                return str(x)

            @toolbox.tool()
            async def async_tool(x: int, context: Context) -> str:
                await context.report_progress(50, 100)
                return str(x)
        """
        # Check if user passed function directly instead of calling decorator
        if callable(name):
            raise TypeError(
                "The @tool decorator was used incorrectly. " "Did you forget to call it? Use @tool() instead of @tool"
            )

        def decorator(fn: Callable) -> Callable:
            self.add_tool(fn, name=name, description=description, version=version)
            return fn

        return decorator