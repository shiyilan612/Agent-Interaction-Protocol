# -*- coding: utf-8 -*-
"""
Created on Thu Apr 24 14:18:14 2025

@author: xmkang, haixinwa
"""
import json
import asyncio
import inspect
import functools
from typing import Dict, Callable, Any
from pydantic import BaseModel, Field, create_model
from atlink_aip.grpc_service.type import ToolInfo, ToolRequest, ToolResponse, Mode, ContentItem

class InvalidSignature(Exception):
    pass

class FuncMetadata:
    def __init__(self, arg_model):
        self.arg_model = arg_model

class ArgModelBase(BaseModel):
    pass

class Tool:
    """
    A high-level Tool module that wraps the ToolServer implementation.
    """
    
    def __init__(self,
                 function: Callable,
                 name: str = None,
                 description: str = None,
                 arguments: Dict[str, Any] =None,
                 version: str = "0.0.1"):
        """
        Initialize a new Tool.
        
        Args:
            function: The function to wrap as a tool
            name: Human-readable name (defaults to function name if not provided)
            description: Tool description (defaults to function docstring if not provided)
            arguments: Optional custom descriptions for the function arguments
            version: Tool version
        """
        if not name:
            name = function.__name__

        # Use function docstring as default description
        if not description and function.__doc__:
            description = inspect.cleandoc(function.__doc__)
        elif not description:
            description = f"Tool based on function {function.__name__}"

        # If custom argument descriptions are not provided, generate them from the signature
        # sig = inspect.signature(function)  # Extract function signature to get arguments
        # if not arguments:
        #     arguments = {
        #         param_name: str(param.annotation) if param.annotation != inspect.Parameter.empty else "any"
        #         for param_name, param in sig.parameters.items()
        #     }
        # else:
        #     # Ensure all function parameters are included in the arguments dictionary
        #     for param_name in sig.parameters:
        #         if param_name not in arguments:
        #             param = sig.parameters[param_name]
        #             arguments[param_name] = str(
        #                 param.annotation) if param.annotation != inspect.Parameter.empty else "any"
        print("function:", function)
        meta = self.func_metadata(function)
        arguments = meta.arg_model.schema()["properties"]

        self.name = name
        self.arguments = arguments
        self.description = description
        self.version = version

        # Set the function executor
        self._func_executor = function

        # Create tool info
        self.tool_info = self._create_tool_info()

    def func_metadata(self, func: Callable[..., Any]) -> FuncMetadata:
        sig = inspect.signature(func)
        params = sig.parameters
        dynamic_pydantic_model_params = {}
        for param in params.values():
            if param.name.startswith("_"):
                raise InvalidSignature(f"Parameter {param.name} of {func.__name__} cannot start with '_'")
            annotation = param.annotation if param.annotation is not inspect.Parameter.empty else Any

            field_info = Field(default=param.default if param.default is not inspect.Parameter.empty else ...)
            dynamic_pydantic_model_params[param.name] = (annotation, field_info)

        arguments_model = create_model(
            f"{func.__name__}Arguments",
            **dynamic_pydantic_model_params,
            __base__=ArgModelBase,
        )
        return FuncMetadata(arg_model=arguments_model)    
    
    def _create_tool_info(self) -> ToolInfo:
        """Create a ToolInfo object for registration with the gateway."""
        tool_info = ToolInfo(
            name=self.name,
            arguments=str(self.arguments),
            description=self.description,
            version=self.version
        )

        return tool_info

    async def _execute_custom_func(self, arguments: Dict[str, Any]) -> Any:
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

    async def update_tool_info(self, name, arguments, description, version):
        """Update tool information with the gateway"""
        # Update local properties
        if name is not None:
            self.name = name
        if description is not None:
            self.description = description
        if arguments is not None:
            self.arguments = arguments
        if version is not None:
            self.version = version

        # Create updated agent info
        self.tool_info = self._create_tool_info()

        return self

    async def invoke_tool(self, request: ToolRequest) -> ToolResponse:
        """
        Invoke the tool.
        Args:
            request: The incoming tool request

        Returns:
            ToolResponse containing the result or error
        """

        try:
            parsed_args = json.loads(request.arguments) if request.arguments else {}

            # Function-based tool
            result = await self._execute_custom_func(parsed_args)
            citem = ContentItem.write_text(str(result))
            response = ToolResponse(
                sender_id="",
                receiver_id=request.sender_id,
                session_id=request.session_id,
                content=[citem],
                is_error=False,
                error_message="No Error"
            )

            return response

        except Exception as e:
            # Handle any exceptions during processing
            return ToolResponse(
                sender_id="",
                receiver_id=request.sender_id,
                session_id=request.session_id,
                content=[],
                is_error=True,
                error_message=f"Error executing tool: {str(e)}"
            )
