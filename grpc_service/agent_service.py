# -*- coding: utf-8 -*-
"""
Created on Wed Apr 19 20:00:00 2025

@author: clleng & haixin
"""

# -*- coding: utf-8 -*-

import grpc
import asyncio
from typing import Dict, Union, AsyncIterable, List
from .utils import ConnectionPool

# Import the generated proto modules
from .schema_pb2_grpc import AgentServiceServicer, add_AgentServiceServicer_to_server
from .schema_pb2_grpc import GatewayServiceStub
from . import schema_pb2 as pb2


class AgentService(AgentServiceServicer):
    """Base AgentService class for handling messages and registration with gateway."""
    
    def __init__(self, 
                 agent_info: pb2.AgentInfo,
                 heartbeat_interval: int = 10):
        """
        Initialize a new Agent instance.
        
        Args:
            agent_info:
                agent_id (str): Unique identifier for the agent.
                address: Address where this tool service will be hosted (e.g., "localhost:50051")
                name (str): Human-readable name of the agent.
                domain (str): Domain of the agent.
                input_mode (schema_pb2.Mode): Input mode of the agent.
                output_mode (schema_pb2.Mode): Output mode of the agent.
                description (str): Description of the agent.
                skills (List[schema_pb2.AgentInfo.AgentSkill]): List of skills for the agent.
                version (str): Version of the agent.
            heartbeat_interval: Interval in seconds between heartbeats
        """
        #creat agent info
        self.agent_info = agent_info
        self.agent_id = self.agent_info.agent_id
        self.address = self.agent_info.address

        # init peers dict
        self._peers: Dict[str, Union[pb2.AgentInfo, pb2.ToolInfo]] = {}

        # init gateway address
        self._gateway_address = None

        # gRPC server for this tool service
        self._server = None

        # stubs of nodes connected to this tool service
        self._connection_pool = ConnectionPool()
        
        # Heartbeat
        self._heartbeat_task = None
        self.heartbeat_interval = heartbeat_interval

    async def _update_peers(self, new_peers):
        """
        Update the list of peers with new information.

        Args:
            new_peers (List[schema_pb2.PeerInfo]): List of new peers to update.
        """
        for peer in new_peers:
            set_field = peer.WhichOneof("info_type")
            if set_field == "agent_info":
                agent_info = peer.agent_info
                self._peers.update({agent_info.agent_id: agent_info})
            elif set_field == "tool_info":
                tool_info = peer.tool_info
                self._peers.update({tool_info.tool_id: tool_info})
            else:
                raise ValueError

    async def _handle_server_termination(self):
        try:
            await self._server.wait_for_termination()
        except Exception as e:
            print(f"Error during server termination: {e}")
        finally:
            await self._server.stop(1)

    async def CallAgent(self,
                        request_iterator: AsyncIterable[pb2.AgentMessage],
                        context:grpc.aio.ServicerContext) -> AsyncIterable[pb2.AgentMessage]:
        """
        Handle incoming agent messages (implements the gRPC service method).

        Args:
            request_iterator (AsyncIterable[schema_pb2.AgentMessage]): Incoming agent messages from the stream.
            context (grpc.aio.ServicerContext): The gRPC context.

        Returns:
            processed_msg (AsyncIterable[schema_pb2.AgentMessage]): Processed agent messages to be sent back.
        """
        pass
    
    
    async def _send_heartbeat(self):
        """Send periodic heartbeats to the gateway."""
        if not self._gateway_address:
            return
        stub = self._connection_pool.get_stub(self._gateway_address)
        
        while True:
            try:
                # Create heartbeat request
                request = pb2.HeartbeatRequest(sender_id=self.agent_id)
                # Send heartbeat
                response = await stub.Heartbeat(request)
                if not response.success:
                    print(f"<{self.agent_id}>: Heartbeat failed: {response.message}")
                else:
                    print(f"<{self.agent_id}>: Heartbeat sent successfully")
                    
                # Wait for the next interval
                await asyncio.sleep(self.heartbeat_interval)
                
            except grpc.aio.AioRpcError as e:
                print(f"<{self.agent_id}>: Heartbeat RPC error: {e.details()}")
                await asyncio.sleep(self.heartbeat_interval)
            except Exception as e:
                print(f"<{self.agent_id}>: Heartbeat error: {str(e)}")
                await asyncio.sleep(self.heartbeat_interval)

    async def start(self):
        """
        Start the Agent service gRPC server.
        
        Args:
            port (int): Port number to listen on.
        """
        self._server = grpc.aio.server()
        add_AgentServiceServicer_to_server(self, self._server)
        self._server.add_insecure_port(self.address)
        await self._server.start()
        print(f"<{self.agent_id}>: Agent {self.agent_id} started on {self.address}")
        asyncio.create_task(self._handle_server_termination())

        return self

    async def stop(self) -> None:
        """Stop the Agent service gRPC server."""
        # Cancel heartbeat task
        if self._heartbeat_task and not self._heartbeat_task.done():
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        
        if self._server:
            await self._server.stop(grace=10.0)
            print(f"Agent service at {self.address} stopped")
        # Close all connections in the pool
        await self._connection_pool.close_all()

    async def connect_to_gateway(self, gateway_address: str):
        """
        Connect to the gateway service and register this Agent.
        
        Args:
            gateway_address: Address of the gateway to register with
        """
        self._gateway_address = gateway_address
        await self._connection_pool.create_stub(self._gateway_address, GatewayServiceStub)
        stub = self._connection_pool.get_stub(self._gateway_address)

        try:
            response = await stub.RegisterAgent(self.agent_info)   # register agent

            await self._update_peers(response.peers) # update peers

            print(f"<{self.agent_id}>: RegisterResponse from GW ({gateway_address})")
            
            # Start heartbeat task after successful registration
            self._heartbeat_task = asyncio.create_task(self._send_heartbeat())

        except grpc.aio.AioRpcError as e:
            print(f"RPC Error: {e.details()}")
            if e.code() == grpc.StatusCode.UNKNOWN:
                # Handle BrokenPipeError
                pass
        except Exception as e:
            print(f"Other exception: {str(e)}")

    async def get_gateway_node(self, domain: str = 'default'):
        """
        Get the list of nodes registered with the gateway.

        Args:
            domain (str): The domain to query for nodes. Defaults to 'default'.
        """
        stub = self._connection_pool.get_stub(self._gateway_address)

        try:
            # query nodes
            response = await stub.GetNodes(pb2.GetNodesRequest(agent_id=self.agent_id, domain=domain))

            # load peers
            await self._update_peers(response.peers)  # update peers

            print(f"<{self.agent_id}>: Update peers from GW ({self._gateway_address})")

        except grpc.aio.AioRpcError as e:
            print(f"RPC Error: {e.details()}")
            if e.code() == grpc.StatusCode.UNKNOWN:
                # 处理 BrokenPipeError
                pass
        except Exception as e:
            print(f"其他异常: {str(e)}")
