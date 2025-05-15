# -*- coding: utf-8 -*-
"""
Created on Thu Apr 24 14:18:14 2025

@author: xmkang
"""

import asyncio
import uuid
import inspect
import functools
from typing import Dict, Callable, Any, Optional, List

from grpc_service.type import ToolInfo, ToolRequest, ToolResponse, Mode, ContentItem
from grpc_service import ToolServiceStub
from module.server import ToolServer
from logger import LoggerManager


class Tool:
    """
    A high-level Tool module that wraps the ToolServer implementation.
    """
    
    def __init__(self, 
                 address: str,
                 tool_id: str = None,
                 name: str = None,
                 domain: str = "default",
                 description: str = "",
                 version: str = "1.0.0",
                 input_mode: List[Mode] = [Mode.TEXT],
                 output_mode: List[Mode] = [Mode.TEXT],
                 executor: Callable = None,
                 arguments: Dict[str, str] = None):
        """
        Initialize a new Tool.
        
        Args:
            address: Address where this tool will be hosted (e.g., "localhost:50051")
            tool_id: Unique identifier for this tool (defaults to UUID if not provided)
            name: Human-readable name for this tool
            domain: Tool group/domain 
            description: Detailed description of the tool's functionality
            version: Tool version
            input_mode: Expected input modality (TEXT, IMAGE, etc.)
            output_mode: Output modality provided by the tool
            executor: An executable function used to complete tasks. It should be defined by user.
            arguments: Dictionary of argument names and their descriptions
        """
        self.address = address
        self.tool_id = tool_id if tool_id else f"tool_{str(uuid.uuid4())}"
        self.name = name if name else self.tool_id
        self.domain = domain
        self.description = description
        self.version = version
        self.input_mode = input_mode
        self.output_mode = output_mode
        self.arguments = arguments or {}
        #function executor
        self._executor = executor
        
        # Create tool info
        self.tool_info = self._create_tool_info()
        
        # Server instance
        self._server = None
        
        # Gateway connection
        self._gateway_address = None
        
        # Request processing function
        self._process_request_func = None
        
        self._logger_mgr = LoggerManager()
        self._logger = self._logger_mgr.get_logger(self.tool_id)
        
    def _create_tool_info(self) -> ToolInfo:
        """Create a ToolInfo object for registration with the gateway."""
        tool_info = ToolInfo(
            tool_id=self.tool_id,
            address=self.address,
            name=self.name,
            domain=self.domain,
            input_mode=self.input_mode,
            output_mode=self.output_mode,
            description=self.description,
            version=self.version,
            arguments=self.arguments
        )
        return tool_info
    
    def set_process_request_handler(self, handler: Callable[[ToolRequest], ToolResponse]):
        """
        Set the function to handle incoming tool requests.
        
        Args:
            handler: A callable function should take a ToolRequest and return a ToolResponse.
            It should be defined by user.
            The handler should cover the executor.
        """
        self._process_request_func = handler
        return self
    
    
    async def _default_process_request_handler(self, request: ToolRequest) -> ToolResponse:
        """
        Default request processing function if none is provided.
        
        Args:
            request: The incoming tool request

        Returns:
            ToolResponse containing the result or error
        """
        self._logger.info(f"<Tool>: Tool {self.tool_id} received a request from "
                          f"{request.sender_id}")
        assert (request.receiver_id == self.tool_id)
        try:
            
            # Process based on tool type
            if self._executor is not None:
                # Function-based tool
                result = await self._execute_custom_func(request.arguments)
                citem = ContentItem.write_text(str(result))
                response = ToolResponse(
                    sender_id=self.tool_id,
                    receiver_id=request.sender_id,
                    session_id=request.session_id,
                    content = [citem],
                    is_error = False,
                    error_message = "No Error"
                )

            # elif self._api_config is not None:
            #     # API-based tool
            #     result = await self._execute_api_call(request.arguments)
            #     response.content = result
            #     response.success = True

            # elif self._mcp_config is not None:
            #     # MCP-based tool
            #     result = await self._execute_mcp_request(request.arguments)
            #     response.content = result
            #     response.success = True

            else:
                # No executor defined
                response.error_message = "No handler configured for this tool"

            return response

        except Exception as e:
            # Handle any exceptions during processing
            return ToolResponse(
                sender_id=self.tool_id,
                receiver_id=request.sender_id,
                success=False,
                error_message=f"Error executing tool: {str(e)}"
            )
        
    
    async def _execute_custom_func(self, arguments: Dict[str, str]) -> Any:
        """Execute the function-based tool handler."""
        if not callable(self._executor):
            raise ValueError("Executor is not callable")

        # Check if handler is a coroutine function
        if inspect.iscoroutinefunction(self._executor):
            result = await self._executor(**arguments)
        else:
            # Run synchronous functions in the executor
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                functools.partial(self._executor, **arguments)
            )
        return result

    
    async def start(self):
        """Start the tool server."""
        if not self._process_request_func:
            self._process_request_func = self._default_process_request_handler
        
        self._server = ToolServer(self.tool_info, self._process_request_func)
        await self._server.start()
        return self
    
    async def stop(self):
        """Stop the tool server."""
        if self._server:
            await self._server.stop()
    
    async def register_to_gateway(self, gateway_address: str):
        """Register to the gateway service."""
        self._gateway_address = gateway_address
        if self._server:
            await self._server.connect_to_gateway(gateway_address)
        return self
    
    @classmethod
    def creat_function_tool(cls,
                            function: Callable,
                            address: str,
                            tool_id: str = None,
                            name: str = None,
                            domain: str = "default",
                            description: str = None,
                            version: str = "1.0.0",
                            input_mode: List[Mode] = [Mode.TEXT],
                            output_mode: List[Mode] = [Mode.TEXT],
                            arguments: Dict[str, Any] = None) -> 'Tool':
        """
        Create a Tool from a Python function.

        Args:
            function: The function to wrap as a tool
            address: Address where this tool service will be hosted
            tool_id: Unique identifier (defaults to function name if not provided)
            name: Human-readable name (defaults to function name if not provided)
            domain: Tool domain/group
            description: Tool description (defaults to function docstring if not provided)
            version: Tool version
            input_mode: Input modality
            output_mode: Output modality
            arguments: Optional custom descriptions for the function arguments

        Returns:
            An initialized Tool instance
        """
        # Use function name as default tool ID and name
        if not tool_id:
            tool_id = function.__name__
        if not name:
            name = function.__name__

        # Use function docstring as default description
        if not description and function.__doc__:
            description = inspect.cleandoc(function.__doc__)
        elif not description:
            description = f"Tool based on function {function.__name__}"

        # Extract function signature to get arguments
        sig = inspect.signature(function)
        # If custom argument descriptions are not provided, generate them from the signature
        if not arguments:
            arguments = {
                param_name: str(param.annotation) if param.annotation != inspect.Parameter.empty else "any"
                for param_name, param in sig.parameters.items()
            }
        else:
            # Ensure all function parameters are included in the arguments dictionary
            for param_name in sig.parameters:
                if param_name not in arguments:
                    param = sig.parameters[param_name]
                    arguments[param_name] = str(
                        param.annotation) if param.annotation != inspect.Parameter.empty else "any"

        # Create the Tool instance
        tool = cls(
            tool_id=tool_id,
            name=name,
            address=address,
            domain=domain,
            description=description,
            version=version,
            input_mode=input_mode,
            output_mode=output_mode,
            executor=function,
            arguments=arguments
        )

        return tool

            
    async def invoke_direct(self, arguments: Dict[str, Any] = None) -> ToolResponse:
        """
        invoke this tool directly without gateway
        """
              #Ensure all key-value in arguments is string
        arguments = {str(k): str(v if v is not None else "N/A") for k, v in arguments.items()}

        # Create the requst
        request = ToolRequest(
            sender_id="direct_caller",
            receiver_id=self.tool_id,
            session_id=f"session_{uuid.uuid4().hex[:8]}",
            tool_name=self.name or "unknown",
            arguments=arguments or {}
        )

        return await self._default_process_request_handler(request)
    
