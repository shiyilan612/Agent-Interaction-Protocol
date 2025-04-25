# -*- coding: utf-8 -*-
"""
Created on Thu Apr 24 16:00:41 2025

@author: xmkang
"""

import asyncio
import uuid
from typing import Dict, List, Optional, Union

from grpc_service.type import AgentInfo, ToolInfo
from grpc_service import GatewayService

class Gateway:
    """
    A high-level Gateway module that wraps the GatewayService implementation,
    providing a central hub for agent and tool registration and communication.
    """
    
    def __init__(self, 
                 address: str,
                 gateway_id: str = None):
        """
        Initialize a new Gateway.
        
        Args:
            address: Address where this gateway will be hosted
            gateway_id: Unique identifier for gateway
        """
        self.address = address
        self.gateway_id = gateway_id if gateway_id else f"gateway_{str(uuid.uuid4())}"
        
        # Create the gateway service
        self._service = None
        
    async def start(self):
        """Start the gateway server."""
        self._service = GatewayService(self.address, self.gateway_id)
        
        # Start the server in a background task
        asyncio.create_task(self._service.start())
        
        # Give it a moment to start up
        await asyncio.sleep(1)
        
        print(f"Gateway {self.gateway_id} started on {self.address}")
        return self
    
    async def stop(self):
        """Stop the gateway server."""
        if self._service:
            await self._service.stop()
    
    def get_registered_nodes(self) -> Dict[str, Union[AgentInfo, ToolInfo]]:
        """
        Get all registered nodes (agents and tools).
        
        Returns:
            Dict of node_id -> node_info
        """
        if not self._service:
            return {}
        
        return self._service._registry
    
    def get_agents(self) -> Dict[str, AgentInfo]:
        """
        Get all registered agents.
        
        Returns:
            Dict of agent_id -> agent_info
        """
        if not self._service:
            return {}
        
        return {node_id: info for node_id, info in self._service._registry.items() 
                if hasattr(info, 'agent_id')}
    
    def get_tools(self) -> Dict[str, ToolInfo]:
        """
        Get all registered tools.
        
        Returns:
            Dict of tool_id -> tool_info
        """
        if not self._service:
            return {}
        
        return {node_id: info for node_id, info in self._service._registry.items() 
                if hasattr(info, 'tool_id')}