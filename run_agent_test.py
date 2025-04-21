"""
       +-------------+
       |   Gateway   |
       | (50051)     |
       +------+------+
              ▲
              | 路由转发
              ▼
+-----------------------------+
|  Agent1        Agent2       |
|  Server:50052  Server:50053 |
|  Client        Client       |
+-----------------------------+
"""
import asyncio
from grpc_service import schema_pb2, AgentService, GatewayService


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


async def main():
    # 启动网关, Agent1, Agent2
    gw_local = GatewayService(address="localhost:50051", gw_id="gw_local")
    agent1 = ExampleAgent(address="localhost:50052", agent_id="agent1")
    agent2 = ExampleAgent(address="localhost:50053", agent_id="agent2")

    asyncio.create_task(gw_local.start())
    asyncio.create_task(agent1.start())
    asyncio.create_task(agent2.start())

    # 确保网关, Agent1, Agent2服务已启动
    await asyncio.sleep(5)

    # 连接网关
    await agent1.connect_to_gateway(gateway_address="localhost:50051")
    await agent2.connect_to_gateway(gateway_address="localhost:50051")

    # 和网关建立流服务
    asyncio.create_task(agent1.create_routed_agent_stream())
    asyncio.create_task(agent2.create_routed_agent_stream())

    # 确保Agent1, Agent2已连接
    await asyncio.sleep(5)

    # 模拟消息发送
    await agent1.send_message("agent2", "Hello world")

    # 保持事件循环运行（否则程序会立即退出）
    await asyncio.sleep(20)


if __name__ == "__main__":
    # run main process
    asyncio.get_event_loop().set_debug(True)
    asyncio.run(main())