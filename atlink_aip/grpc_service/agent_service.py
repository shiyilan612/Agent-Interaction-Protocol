# -*- coding: utf-8 -*-
"""
Created on Wed Apr 19 20:00:00 2025

@author: clleng & haixin & xmkang
"""

# -*- coding: utf-8 -*-

import grpc
import asyncio
from typing import Dict, Union, AsyncIterable, List
from .utils import ConnectionPool
from ..logger import LoggerManager

# Import the generated proto modules
from .schema_pb2_grpc import AgentServiceServicer, add_AgentServiceServicer_to_server
from .schema_pb2_grpc import GatewayServiceStub
from . import schema_pb2 as pb2


class AgentService(AgentServiceServicer):
    """Base AgentService class for handling messages and registration with gateway."""
    
    def __init__(self,
                 agent_info: pb2.AgentInfo,
                 heartbeat_interval: int = 10
                 ):
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
        """
        #creat agent info
        self.agent_info = agent_info
        self.agent_id = self.agent_info.agent_id
        self.address = self.agent_info.address

        # init peers dict
        self._peers: Dict[str, Union[pb2.AgentInfo, pb2.ToolBoxInfo]] = {}

        # init gateway address
        self._gateway_address = None

        # gRPC server for this tool service
        self._server = None

        self._logger_mgr = LoggerManager()
        self._logger = self._logger_mgr.get_logger(self.agent_id)
        
        # stubs of nodes connected to this tool service
        self._connection_pool = ConnectionPool(self._logger)

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
            elif set_field == "toolbox_info":
                toolbox_info = peer.toolbox_info
                self._peers.update({toolbox_info.toolbox_id: toolbox_info})
            else:
                raise ValueError

    async def _handle_server_termination(self):
        try:
            await self._server.wait_for_termination()
        except Exception as e:
            self._logger.error(f"<Agent>: Failed to terminate gRPC server - Exception: {e}")
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
                    self._logger.debug(f"<{self.agent_id}>: Heartbeat failed: {response.message}")
                else:
                    self._logger.debug(f"<{self.agent_id}>: Heartbeat sent successfully")

                # Wait for the next interval
                await asyncio.sleep(self.heartbeat_interval)

            except grpc.aio.AioRpcError as e:
                self._logger.error(f"<{self.agent_id}>: Heartbeat RPC error: {e.details()}")
                await asyncio.sleep(self.heartbeat_interval)
            except Exception as e:
                self._logger.error(f"<{self.agent_id}>: Heartbeat error: {str(e)}")
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
        self._logger.info(f"<Agent>: Agent [{self.agent_id}] server started on [{self.address}]")
        asyncio.create_task(self._handle_server_termination())

        return self

    async def stop(self) -> None:
        """Stop the Agent service gRPC server."""
        if self._server:
            await self._server.stop(grace=10.0)
            await self.disconnect_from_gateway()
            self._logger.info(f"<Agent>: Agent service [{self.agent_id}] at [{self.address}] "
                              f"stopped")
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

            self._logger.info(f"<Agent>: {'Registered' if response else 'Failed to register'}"
                              f" to Gateway at [{gateway_address}]")

            self._heartbeat_task = asyncio.create_task(self._send_heartbeat())

        except grpc.aio.AioRpcError as e:
            self._logger.error(f"<Agent>: Failed to register to Gateway at [{gateway_address}] - "
                               f"RPC Error: {e.code()}, details: {e.details()}")
            if e.code() == grpc.StatusCode.UNKNOWN:
                # Handle BrokenPipeError
                pass
            raise
        except Exception as e:
            self._logger.error(f"<Agent>: Failed to register to Gateway at [{gateway_address}] - "
                               f"Exception: {str(e)}")
        
    async def disconnect_from_gateway(self):
        """
        Disconnect from the gateway service and deregister this Agent.
        """
        stub = self._connection_pool.get_stub(self._gateway_address)

        try:
            # deregister agent    
            response = await stub.DeregisterNode(pb2.DeregisterNodeRequest(node_id=self.agent_id))         
            await self._connection_pool.close_stub(self._gateway_address)
            
            self._logger.info(f"<Agent>: {'Deregistered' if response else 'Failed to deregister'}"
                              f" from Gateway at [{self._gateway_address}]")

            # Cancel heartbeat task
            if self._heartbeat_task and not self._heartbeat_task.done():
                self._heartbeat_task.cancel()
                try:
                    await self._heartbeat_task
                except asyncio.CancelledError:
                    pass
            
            self._gateway_address = None

        except grpc.aio.AioRpcError as e:
            self._logger.error(f"<Agent>: Failed to deregister from Gateway at [{self._gateway_address}] - "
                               f"RPC Error: {e.code()}, details: {e.details()}")
            if e.code() == grpc.StatusCode.UNKNOWN:
                # Handle BrokenPipeError
                pass
            raise
        except Exception as e:
            self._logger.error(f"<Agent>: Failed to deregister from Gateway at [{self._gateway_address}] - "
                               f"Exception: {str(e)}")
            
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

            self._logger.info(f"<Agent>: Updated peers from Gateway at [{self._gateway_address}]")

        except grpc.aio.AioRpcError as e:
            self._logger.error(f"<Agent>: Failed to get node information from Gateway "
                               f"at [{self._gateway_address}] - "
                               f"RPC Error: {e.code()}, details: {e.details()}")
            if e.code() == grpc.StatusCode.UNKNOWN:
                # 处理 BrokenPipeError
                pass
        except Exception as e:
            self._logger.error(f"<Agent>: Failed to get node information from Gateway "
                               f"at [{self._gateway_address}] - Exception: {str(e)}")
