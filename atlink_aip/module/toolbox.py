# -*- coding: utf-8 -*-
"""
Created on Tue Jun 17 9:36:12 2025

@author: clleng, haixinwa
"""

import uuid
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
                 address: str,
                 name: str = None,
                 toolbox_id: str = None,
                 domain: str = "default",
                 description: str = "",
                 warn_on_duplicate_tools: bool = True,
                 tools: List[Tool] | None = None):
        """
        Initialize a new ToolBox.
        
        Args:
            address: Address where this toolbox will be hosted (e.g., "localhost:50051")
            toolbox_id: Unique identifier for this toolbox (defaults to UUID if not provided)
            name: Human-readable name for this toolbox
            domain: toolbox group/domain 
            description: Detailed description of the toolbox
            warn_on_duplicate_tools: Whether to log warnings for duplicate tool
            tools: Optional list of initial tools to register in this toolbox
        """
        self.address = address
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
        
        # Update tools information
        self._update_toolbox_info()
        
        # Server instance
        self._server = None
        
        # Gateway connection
        self._gateway_address = None
        
    def _collect_tools_info(self) -> List[ToolInfo]:
        """Collect and return a list of ToolInfo objects for each tool contained in the ToolBox."""
        tools_info = [tool.tool_info for tool in self._tools.values()]
        return tools_info
    
    def _create_toolbox_info(self) -> ToolBoxInfo:
        """Create a ToolBoxInfo object for registration with the gateway."""
        toolbox_info = ToolBoxInfo(
            toolbox_id=self.toolbox_id,
            address=self.address,
            name=self.name,
            domain=self.domain,
            description=self.description,
            tools=self.tools_info
        )
        return toolbox_info
    
    def _update_toolbox_info(self) -> None:
        """Update the toolbox information when tools change."""
        self.tools_info = self._collect_tools_info()
        self.toolbox_info = self._create_toolbox_info()

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

    async def register_to_gateway(self, gateway_address: str):
        """Register to the gateway service."""
        self._gateway_address = gateway_address
        if self._server:
            await self._server.connect_to_gateway(gateway_address)
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

    async def update_tool_info(self,
                               name=None,
                               description=None,
                               version=None,
                               arguments=None):
        """Update tool information with the gateway"""

        tool = self._tools.get(self.name)
        if not tool:
            self._logger.error(f"<ToolBox>: Tool [{self.name}] not found in toolbox.")
        else:
            await tool.update_tool_info(name, description, arguments, version)
            self._update_toolbox_info()

            # Pass to server
            if self._server:
                await self._server.update_toolbox_info(self.toolbox_info)

        return self