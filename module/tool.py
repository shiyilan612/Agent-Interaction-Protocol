# -*- coding: utf-8 -*-
"""
Created on Thu Apr 24 14:18:14 2025

@author: xmkang
"""

import asyncio
import uuid
import inspect
import functools
import aiohttp
import json
from typing import Dict, Callable, Any, Optional, List, Union

from grpc_service.type import ToolInfo, ToolRequest, ToolResponse, Mode, ContentItem
from grpc_service import ToolServiceStub
from module.server import ToolServer



class APIConfig:
    """Configuration for API-based tools."""
    
    def __init__(self, 
                 url: str,
                 method: str = "GET",
                 headers: Dict[str, str] = None,
                 timeout: int = 30):
        """
        Initialize API configuration.
        
        Args:
            url: The base URL for the API endpoint
            method: HTTP method (GET, POST, PUT, DELETE, etc.)
            headers: HTTP headers to include in the request
            timeout: Request timeout in seconds
        """
        self.url = url
        self.method = method.upper()
        self.headers = headers or {}
        self.timeout = timeout


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
        self._func_executor = None
        #api call config
        self._api_config = None
        
        # Create tool info
        self.tool_info = self._create_tool_info()
        
        # Server instance
        self._server = None
        
        # Gateway connection
        self._gateway_address = None
        
        # Request processing function
        self._process_request_func = None
        
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
    
    
    
    async def update_tool_info(self, 
                              name=None, 
                              description=None, 
                              domain=None,
                              version=None, 
                              input_mode=None, 
                              output_mode=None, 
                              arguments=None):
        """Update tool information with the gateway"""
        # Update local properties
        if name is not None:
            self.name = name
        if description is not None:
            self.description = description
        if domain is not None:
            self.domain = domain
        if version is not None:
            self.version = version
        if input_mode is not None:
            self.input_mode = input_mode
        if output_mode is not None:
            self.output_mode = output_mode  
        if arguments is not None:
            self.arguments = arguments
        
        # Create updated agent info
        updated_tool_info = self._create_tool_info()
        
        # Pass to server
        if self._server:
            await self._server.update_tool_info(updated_tool_info)
        
        return self
    
    
    
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
        print(f"Tool {self.tool_id} received request")
        assert (request.receiver_id == self.tool_id)
        try:
            
            # Process based on tool type
            if self._func_executor is not None:
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

            elif self._api_config is not None:
                # API-based tool
                result = await self._execute_api_call(request.arguments)
                if isinstance(result, str):
                    citem = ContentItem.write_text(result)
                else:
                    citem = ContentItem.write_text(json.dumps(result, indent=2))
                response = ToolResponse(
                    sender_id=self.tool_id,
                    receiver_id=request.sender_id,
                    session_id=request.session_id,
                    content=[citem],
                    is_error=False,
                    error_message="No Error"
                )

            # elif self._mcp_config is not None:
            #     # MCP-based tool
            #     result = await self._execute_mcp_request(request.arguments)
            #     response.content = result
            #     response.success = True

            else:
                # No executor or api_config defined
                response = ToolResponse(
                    sender_id=self.tool_id,
                    receiver_id=request.sender_id,
                    session_id=request.session_id,
                    content=[],
                    is_error=True,
                    error_message="No handler configured for this tool"
                )

            return response

        except Exception as e:
            # Handle any exceptions during processing
            return ToolResponse(
                sender_id=self.tool_id,
                receiver_id=request.sender_id,
                session_id=request.session_id,
                content=[],
                is_error=True,
                error_message=f"Error executing tool: {str(e)}"
            )
        
    
    async def _execute_custom_func(self, arguments: Dict[str, str]) -> Any:
        """Execute the function-based tool handler."""
        if not callable(self._func_executor):
            raise ValueError("Executor is not callable")

        # Check if handler is a coroutine function
        if inspect.iscoroutinefunction(self._func_executor):
            result = await self._func_executor(**arguments)
        else:
            # Run synchronous functions in the executor
            loop = asyncio.get_event_loop()
            result = await loop.run_in_executor(
                None,
                functools.partial(self._func_executor, **arguments)
            )
        return result
    

    async def _execute_api_call(self, arguments: Dict[str, str]) -> Any:
        """Execute the API call with the provided arguments."""
        if not self._api_config:
            raise ValueError("API configuration is not set")
            
        # Process URL - replace {param} placeholders with argument values
        url = self._api_config.url
        for arg_name, arg_value in arguments.items():
            placeholder = f"{{{arg_name}}}"
            if placeholder in url:
                url = url.replace(placeholder, arg_value)
        
        # Execute the HTTP request
        async with aiohttp.ClientSession() as session:
            method = self._api_config.method.lower()
            request_kwargs = {
                "headers": dict(self._api_config.headers),
                "timeout": self._api_config.timeout
            }
            
            # For GET requests, add arguments not used in URL as query parameters
            if method == "get":
                query_params = {}
                for arg_name, arg_value in arguments.items():
                    if f"{{{arg_name}}}" not in self._api_config.url:
                        query_params[arg_name] = arg_value
                if query_params:
                    request_kwargs["params"] = query_params
            
            # For other methods (POST, PUT, etc.), add unused arguments to request body
            elif method in ["post", "put", "patch"]:
                body_params = {}
                for arg_name, arg_value in arguments.items():
                    if f"{{{arg_name}}}" not in self._api_config.url:
                        body_params[arg_name] = arg_value
                if body_params:
                    request_kwargs["json"] = body_params
            
            # Execute the request
            http_method = getattr(session, method)
            async with http_method(url, **request_kwargs) as response:
                # Check if the request was successful
                response.raise_for_status()
                
                # Process the response based on content type
                content_type = response.headers.get("Content-Type", "")
                if "application/json" in content_type:
                    result = await response.json()
                else:
                    result = await response.text()
                    
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
    def create_function_tool(cls,
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
            arguments=arguments
        )
        
        # Set the function executor
        tool._func_executor = function

        return tool

    
    @classmethod
    def create_api_tool(cls,
                       address: str,
                       api_url: str,
                       api_method: str = "GET",
                       api_headers: Dict[str, str] = None,
                       api_timeout: int = 30,
                       tool_id: str = None,
                       name: str = None,
                       domain: str = "default",
                       description: str = "",
                       version: str = "1.0.0",
                       input_mode: List[Mode] = [Mode.TEXT],
                       output_mode: List[Mode] = [Mode.TEXT],
                       arguments: Dict[str, str] = None) -> 'Tool':
        """
        Create a Tool from an API configuration.
        
        Args:
            address: Address where this tool service will be hosted
            api_url: The API endpoint URL (can contain placeholders like {param_name})
            api_method: HTTP method (GET, POST, PUT, DELETE, etc.)
            api_headers: HTTP headers to include in the request
            api_timeout: Request timeout in seconds
            tool_id: Unique identifier for this tool
            name: Human-readable name for this tool
            domain: Tool domain/group
            description: Tool description
            version: Tool version
            input_mode: Input modality
            output_mode: Output modality
            arguments: Dictionary of argument names and their descriptions
            
        Returns:
            An initialized Tool instance configured for API calls
        """
        # Generate a default tool_id and name if not provided
        if not tool_id:
            base_name = api_url.split("/")[-1] if api_url.split("/")[-1] else api_url.split("/")[-2]
            tool_id = f"api_{base_name}_{str(uuid.uuid4())[:8]}"
        if not name:
            name = tool_id
        # Extract arguments from URL placeholders if not provided
        if not arguments:
            arguments = {}
            # Look for {param_name} patterns in the URL
            import re
            placeholders = re.findall(r"\{([^}]+)\}", api_url)
            for placeholder in placeholders:
                arguments[placeholder] = f"Parameter for {placeholder}"
                        
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
            arguments=arguments
        )
        
        # Set the API configuration
        tool._api_config = APIConfig(
            url=api_url,
            method=api_method,
            headers=api_headers,
            timeout=api_timeout
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
        
        
