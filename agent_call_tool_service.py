"""
       +-------------+
       |   Gateway   |
       | (50051)     |
       +------+------+
              ▲
              | 路由转发
              ▼
+-----------------------------+
|  Agent1        SumCalculator|
|  Server:50052  Server:50054 |
|  Client        Client       |
+-----------------------------+
"""
from session.tool_session import Tool
import asyncio
from grpc_service import schema_pb2, AgentService, GatewayService
from typing import Dict

class ExampleAgent(AgentService):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.message_queue = asyncio.Queue()

    async def handle_outgoing_message(self) -> schema_pb2.AgentMessage:
        """实现RouteMessage消息发送逻辑"""
        message = await self.message_queue.get()
        return  message

    async def handle_incoming_message(self, message: schema_pb2.AgentMessage):
        """实现RouteMessage消息接收逻辑"""
        print(f"<{self.agent_id}>: {message.text}")

    async def process_agent_message(self, message: schema_pb2.AgentMessage) -> schema_pb2.AgentMessage:
        """实现StreamCommunicate消息处理逻辑"""
        print(f"<{self.agent_id}>: receive \"{message.text}\" from {message.sender_id}")

        processed_message = schema_pb2.AgentMessage(
            sender_id=self.agent_id,
            receiver_id=message.sender_id,
            text=f"Reply from {self.agent_id}: \"{message.text}\""
        )

        return processed_message

    async def send_message(self, receiver_id: str, text: str):
        print(f"<{self.agent_id}>: send \"{text}\" to {receiver_id}")

        message = schema_pb2.AgentMessage(
            sender_id=self.agent_id,
            receiver_id=receiver_id,
            text=text
        )
        await self.message_queue.put(message)

    async def call_tool(self, receiver_id: str, tool_name: str, arguments: Dict[str, str]):
        print(f"<{self.agent_id}>: call tool <{tool_name}> with arguments {arguments}")

        tool_request = schema_pb2.ToolRequest(
            sender_id=self.agent_id,
            receiver_id=receiver_id,
            tool_name=tool_name,
            arguments=arguments
        )
        response = await self.Call_tool_by_route(tool_request)
        print(f"<{self.agent_id}>: tool <{tool_name}> response: {response.text}")


async def main():
    gw_local = GatewayService(gw_id="gw_local")
    agent1 = ExampleAgent(agent_id="agent1")

    asyncio.create_task(gw_local.start(port=50051))
    asyncio.create_task(agent1.start(port=50052))

    await asyncio.sleep(5)

    # 连接网关
    await agent1.connect_to_gateway(gateway_addr="localhost:50051")
    
    # Example: Creating a function-based tool
    async def calculate_sum(a:int, b:int) -> int:
        """Adds two numbers and returns the sum."""
        return int(a) + int(b)

    sum_tool = Tool.creat_function_tool(
        function=calculate_sum,
        address="localhost:50054",
        gateway_address="localhost:50051",
        name="SumCalculator",
        description="A simple tool that adds two numbers"
    )
    
    tool_task = asyncio.create_task(sum_tool.run())
    await asyncio.sleep(5)

    # invoke tool by agent through gateway
    await agent1.call_tool(receiver_id="calculate_sum", tool_name="SumCalculator", arguments={"a":"5", "b":"3"})

if __name__ == "__main__":
    # run main process
    asyncio.get_event_loop().set_debug(True)
    asyncio.run(main())
