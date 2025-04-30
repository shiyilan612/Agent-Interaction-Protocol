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

# Import the generated proto modules
from .schema_pb2_grpc import AgentServiceServicer, add_AgentServiceServicer_to_server
from .schema_pb2_grpc import GatewayServiceStub
from . import schema_pb2 as pb2


class AgentService(AgentServiceServicer):
    """Base AgentService class for handling messages and registration with gateway."""
    
    def __init__(self, 
                 agent_info: pb2.AgentInfo,
                 heartbeat_interval: int = 10,
                 update_check_interval: int = 10):
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
            update_check_interval: Interval in seconds between checking for self info updates
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
        
        #Update
        # self Info update
        self._update_check_task = None
        self.update_check_interval = update_check_interval
        self._last_info_hash = self._hash_agent_info(self.agent_info) #hash of the history agent info for detecting changes
        #other nodes update subscription
        self._update_subscription_task = None
        self._subscribed_node_ids = set()  # Specific nodes to subscribe to



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
                
                
    def _hash_agent_info(self, agent_info: pb2.AgentInfo):
        """
        Create a hash of agent info for detecting changes.
        Args:
            agent_info: Agent information to hash
            
        Returns:
            Hash value as integer
        """
        hash_components = [
            agent_info.agent_id,
            agent_info.name,
            agent_info.domain,
            agent_info.description,
            agent_info.version,
            str(agent_info.input_mode),
            str(agent_info.output_mode),
            str([skill.skill_id + skill.capability for skill in agent_info.skills])
        ]
        return hash("".join(hash_components))
                
    async def _check_agent_info_updates(self):
        """Periodically check if agent info has changed and notify gateway if needed."""
        if not self._gateway_address:
            return
        stub = self._connection_pool.get_stub(self._gateway_address)
        
        while True:
            try:
                # Check if agent info has changed
                current_hash = self._hash_agent_info(self.agent_info)
                if current_hash != self._last_info_hash:
                    print(f"<{self.agent_id}>: Agent info changed, updating gateway")
                    self._last_info_hash = current_hash
                    
                    # Create update request
                    peer = pb2.Peer()
                    peer.agent_info.CopyFrom(self.agent_info)
                    request = pb2.UpdateNodeInfoRequest(
                        sender_id = self.agent_id,
                        peer = peer
                    )
                    # Send update request to the gateway
                    response = await stub.UpdateNodeInfo(request)
                    if not response.success:
                        print(f"<{self.agent_id}>: Update failed: {response.message}")
                    else:
                        print(f"<{self.agent_id}>: Agent info updated successfully")
                
                # Wait for the next interval
                await asyncio.sleep(self.update_check_interval)
                
            except grpc.aio.AioRpcError as e:
                print(f"<{self.agent_id}>: Update check RPC error: {e.details()}")
                await asyncio.sleep(self.update_check_interval)
            except Exception as e:
                print(f"<{self.agent_id}>: Update check error: {str(e)}")
                await asyncio.sleep(self.update_check_interval)
                
                
    async def _subscribe_to_updates(self, node_ids: List[str] = list()):
        """
        Subscribe to node updates from the gateway. When reviced update message, update self._peers
        If empty, subscribe to all nodes
        
        Args:
            node_ids: Optional list of node IDs to subscribe to. If None, use self._subscribed_node_ids.

        """
        if not self._gateway_address:
            return 
        
        stub = self._connection_pool.get_stub(self._gateway_address)
        
        # Use provided node_ids or fall back to instance variable
        nodes_to_subscribe = node_ids if len(node_ids) > 0 else list(self._subscribed_node_ids)
    
        
        # Create subscription request
        request = pb2.UpdateSubscriptionRequest(
            subscriber_id=self.agent_id,
            node_ids=nodes_to_subscribe #If empty, subscribe to all nodes
        )
        
        try:
            # Send subscription request
            async for update_message in stub.SubscribeToUpdates(request):
                # Process received update
                update_type = update_message.update_type
                node_id = update_message.node_id
                if update_type == pb2.NodeUpdate.ADDED or update_type == pb2.NodeUpdate.UPDATED:
                    # Update local peer information
                    await self._update_peers([update_message.peer])
                    print(f"<{self.agent_id}>: Received {pb2.NodeUpdate.UpdateType.Name(update_type)} update for node {node_id}")
                elif update_type == pb2.NodeUpdate.REMOVED:
                    # Remove from local peers dictionary
                    if node_id in self._peers:
                        del self._peers[node_id]
                        print(f"<{self.agent_id}>: Node {node_id} removed from peers")
                
        except grpc.aio.AioRpcError as e:
            print(f"<{self.agent_id}>: Update subscription error: {e.details()}")
            # Try to re-establish subscription after some delay
            await asyncio.sleep(5)
            if not self._update_subscription_task.done():
                asyncio.create_task(self._subscribe_to_updates())
        except Exception as e:
            print(f"<{self.agent_id}>: Subscription error: {str(e)}")
            await asyncio.sleep(5)
            if not self._update_subscription_task.done():
                asyncio.create_task(self._subscribe_to_updates())



    async def subscribe_to_nodes(self, node_ids: List[str] = list()) -> bool:
        """
        Send a subscription request to the gateway.
        
        Args:
            node_ids: List of node IDs to subscribe to.
                     Empty list means subscribe to all nodes.
                     
        Returns:
            Success status
        """
        if not self._gateway_address:
            print(f"<{self.agent_id}>: No gateway connection established")
            return False
            
        # Use current subscriptions if none provided
        node_id_list = list(self._subscribed_node_ids) if len(node_ids) == 0 else node_ids
        
        # Restart subscription task with the new list
        if self._update_subscription_task:
            self._update_subscription_task.cancel()
            try:
                await self._update_subscription_task
            except asyncio.CancelledError:
                pass
        
        # Start new subscription task with the specified node_ids
        self._update_subscription_task = asyncio.create_task(self._subscribe_to_updates(node_id_list))
        
        return True



    async def unsubscribe_from_nodes(self, node_ids: List[str] = list()) -> bool:
        """
        Send an unsubscribe request to the gateway.
        Remove the unsubscribed nodes, and then restart new subscribe
        
        Args:
            node_ids: List of node IDs to unsubscribe from. 
                      If empty, unsubscribe from all nodes.
                     
        Returns:
            Success status
        """
        if not self._gateway_address:
            print(f"<{self.agent_id}>: No gateway connection established")
            return False

            
        stub = self._connection_pool.get_stub(self._gateway_address)
        
        try:            
            # Create unsubscribe request
            request = pb2.UnsubscribeRequest(
                subscriber_id=self.agent_id,
                node_ids=node_ids
            )
            
            # Send unsubscribe request to gateway
            response = await stub.Unsubscribe(request)
            
            if response.success:
                print(f"<{self.agent_id}>: Successfully unsubscribed from nodes: {response.message}")
                
                # Update local subscription state if needed
                if not node_ids:  # If unsubscribing from all
                    self._subscribed_node_ids.clear()
                else:
                    for node_id in node_ids:
                        if node_id in self._subscribed_node_ids:
                            self._subscribed_node_ids.remove(node_id)
                
                # Restart subscription task with updated subscriptions
                if self._update_subscription_task:
                    self._update_subscription_task.cancel()
                    try:
                        await self._update_subscription_task
                    except asyncio.CancelledError:
                        pass
                    if self._subscribed_node_ids:  # Only restart if still have subscriptions
                        self._update_subscription_task = asyncio.create_task(self._subscribe_to_updates())
                
                return True
            else:
                print(f"<{self.agent_id}>: Failed to unsubscribe: {response.message}")
                return False
                
        except grpc.aio.AioRpcError as e:
            print(f"<{self.agent_id}>: Unsubscribe RPC error: {e.details()}")
            return False
        except Exception as e:
            print(f"<{self.agent_id}>: Unsubscribe error: {str(e)}")
            return False


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
        # Cancel all background tasks
        for task in [self._heartbeat_task, self._update_check_task, self._update_subscription_task]:
            if task and not task.done():
                task.cancel()
                try:
                    await task
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
            
            # Start background tasks after successful registration
            self._heartbeat_task = asyncio.create_task(self._send_heartbeat())
            self._update_check_task = asyncio.create_task(self._check_agent_info_updates())
            self._update_subscription_task = asyncio.create_task(self._subscribe_to_updates())

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
