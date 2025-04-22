# -*- coding: utf-8 -*-
"""
Created on Fri Apr 19 20:00:00 2025

@author: clleng & haixin
"""

# -*- coding: utf-8 -*-

import grpc
import uuid
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
                 address: str,
                 agent_id: str = None,
                 name: str = None,
                 domain: str = 'default',
                 input_mode: pb2.Mode = pb2.Mode.TEXT,
                 output_mode: pb2.Mode = pb2.Mode.TEXT,
                 description: str = '',
                 skills: List[pb2.AgentInfo.AgentSkill] = [],
                 version: str = '1.0.0' ):
        """
        Initialize a new Agent instance.
        
        Args:
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
        self.address = address
        self.agent_id = agent_id if agent_id else f"agent_{str(uuid.uuid4())}"
        self.name = name if name else self.agent_id
        self.domain = domain
        self.input_mode = input_mode
        self.output_mode = output_mode
        self.description = description
        self.skills = skills
        self.version = version  

        #creat agent info
        self.agent_info = self._create_agent_info()

        # init peers dict
        self._peers: Dict[str, Union[pb2.AgentInfo, pb2.ToolInfo]] = {}

        # init gateway address
        self._gateway_address = None

        # gRPC server for this tool service
        self._server = None

        # stubs of nodes connected to this tool service
        self._connection_pool = ConnectionPool()

    async def process_agent_message(self, message: pb2.AgentMessage) -> pb2.AgentMessage:
        """子类需要实现CallAgent消息处理逻辑"""
        raise NotImplementedError

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
    
    def _create_agent_info(self) -> pb2.AgentInfo:
        """
        Create a AgentInfo message for registration with the gateway.
        """
        agent_info = pb2.AgentInfo(
            agent_id=self.agent_id,
            address=self.address,
            name=self.name,
            domain=self.domain,
            input_mode=self.input_mode,
            output_mode=self.output_mode,
            description=self.description,
            skills=self.skills,
            version=self.version
        )
        
        return agent_info

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
        async for message in request_iterator:
            # process incoming message asynchronously
            processed_msg = await self.process_agent_message(message)
            # send back the processed message
            yield processed_msg

    async def start(self):
        """
        Start the Agent service gRPC server.
        
        Args:
            port (int): Port number to listen on.
        """
        self._server = grpc.aio.server()
        add_AgentServiceServicer_to_server(self, self._server)
        # 【TODO】这里还是暂时用 localhost 作为默认地址
        self._server.add_insecure_port(self.address)
        self.agent_info = self._create_agent_info()
        await self._server.start()
        print(f"<{self.agent_id}>: Agent {self.agent_id} started on {self.address}")

        # Wait for termination
        try:
            await self._server.wait_for_termination()
        finally:
            # Ensure proper server shutdown
            await self._server.stop(1)  # 1 second timeout

    async def stop(self) -> None:
        """Stop the Agent service gRPC server."""
        if self._server:
            await self._server.stop(grace=None)
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
