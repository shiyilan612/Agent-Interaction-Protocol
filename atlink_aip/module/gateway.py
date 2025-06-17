# -*- coding: utf-8 -*-
"""
Created on Thu Apr 24 16:00:41 2025

@author: xmkang & clleng
"""

import uuid
from typing import Dict, List, Union

from ..grpc_service.type import AgentInfo, ToolBoxInfo
from ..module.host import GatewayHost
from ..logger import LoggerManager

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
        self._host = None
        
        self._logger_mgr = LoggerManager()
        self._logger = self._logger_mgr.get_logger(self.gateway_id)

    async def start(self):
        """Start the gateway server."""
        self._host = GatewayHost(self.address, self.gateway_id)

        # Start the server
        await self._host.start()

    async def stop(self):
        """Stop the gateway server."""
        if self._host:
            await self._host.stop()
            
        # Stop loggers
        self._logger_mgr.stop()

    async def connect_to_gateway(self, gateway_address: str):
        """Connect to another gateway.

        Args:
            gateway_address: Address of the gateway to connect to
        """
        # Prepared for gateway interconnection
        # This is a placeholder for the actual implementation
        pass

    async def get_registered_nodes(self) -> Dict[str, Union[AgentInfo, ToolBoxInfo]]:
        """
        Get all registered nodes (agents and tools).

        Returns:
            Dict of node_id -> node_info
        """
        if not self._host:
            return {}

        nodes_info = {}
        nodes_info.update(await self._host.get_agents_info())
        tools_info = await self._host.get_tools_info()
        nodes_info.update(tools_info)

        return nodes_info

    async def get_registered_agents(self) -> Dict[str, AgentInfo]:
        """
        Get all registered agents.

        Returns:
            Dict of agent_id -> agent_info
        """
        if not self._host:
            return {}

        agents_info = await self._host.get_agents_info()
        return agents_info

    async def get_registered_tools(self) -> Dict[str, ToolBoxInfo]:
        """
        Get all registered toolboxes.

        Returns:
            Dict of toolbox_id -> toolbox_info
        """
        if not self._host:
            return {}

        toolboxes_info = await self._host.get_tools_info()
        return toolboxes_info