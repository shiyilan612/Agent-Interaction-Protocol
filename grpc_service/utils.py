# -*- coding: utf-8 -*-
"""
Created on Wed Apr 16 15:00:00 2025

@author: haixinwa & clleng
"""

import grpc
import asyncio
from typing import Type, Dict, Union, Optional
from .schema_pb2_grpc import GatewayServiceStub, AgentServiceStub, ToolServiceStub


class ConnectionPool:
    """gRPC连接池管理"""
    def __init__(self):
        self._channels: Dict[str, grpc.aio.Channel] = {}
        self._stubs: Dict[str, Union[GatewayServiceStub, AgentServiceStub, ToolServiceStub]] = {}

    async def create_stub(self, address: str,
                          stub_ptr: Union[Type[GatewayServiceStub], Type[AgentServiceStub], Type[ToolServiceStub]]):
        """ create a stub for the input address"""
        try:
            channel = grpc.aio.insecure_channel(address)
            await channel.channel_ready()
            self._channels[address] = channel
            self._stubs[address] = stub_ptr(channel)
        except grpc.RpcError as e:
            print(f"Connection failed to {address}: {e.code()}")

    async def close_stub(self, address: str):
        """ close a connection for the input address"""
        try:
            channel = self._channels.get(address, None)
            if channel:
                await channel.close()
                del self._channels[address]
                del self._stubs[address]
        except grpc.RpcError as e:
            print(f"Failed to close connection to {address}: {e.code()}")

    def get_stub(self, address: str) -> Optional[Union[GatewayServiceStub, AgentServiceStub, ToolServiceStub]]:
        """get a stub for a specified address"""
        if address in self._stubs:
            return self._stubs[address]
        else:
            print(f"None Connection to {address}")
            return None

    async def close_all(self):
        """close all connections"""
        closing_tasks = []
        for addr, channel in self._channels.items():
            closing_tasks.append(channel.close())
        await asyncio.gather(*closing_tasks, return_exceptions=True)
        self._channels.clear()
        self._stubs.clear()
