import time
import asyncio
from typing import Dict
from grpc_service.schema_pb2 import AgentMessage


class AgentSession:
    def __init__(self):
        self.active_sessions: Dict[str, asyncio.Future] = {}
        self.timeout = 30

    def _generate_session_id(self, agent_id) -> str:
        return f"{agent_id}_{time.strftime('%Y%m%d_%H%M%S', time.localtime())}"

    async def create_session(self, receiver_id: str, content: str) -> AgentMessage:
        pass

    def handle_response(self, response: AgentMessage):
        pass