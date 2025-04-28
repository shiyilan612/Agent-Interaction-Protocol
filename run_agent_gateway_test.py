import uuid
import asyncio

from grpc_service.type import AgentMessage, AgentInfo, TaskInfo, AgentSkill, SessionStatus, TaskStatus, Mode, ContentItem
from module.client import  AgentClient
from module.server import AgentServer
from module.host import GatewayHost
from module import Gateway


async def main():
    async def process_request_func(message: AgentMessage) -> AgentMessage:
        receiver_id = message.receiver_id
        sender_id = message.sender_id
        content = message.content
        status = message.session_status
        print(f"<{receiver_id}>: receive \"{content[0]._text}\" from {sender_id} in session: {message.session_id}")

        message.content = [ContentItem(text= f"Response to message [{content[0]._text}]")]
        message.sender_id = receiver_id
        message.receiver_id = sender_id
        message.task_info.task_status = TaskStatus.EXECUTING
        message.session_status = SessionStatus.HOLD_QUEST
        if status == SessionStatus.STOP_QUEST:
            message.session_status = SessionStatus.STOP_RESPONSE
            message.task_info.task_status = TaskStatus.FINISH

        await asyncio.sleep(1)

        return message

    agent_info_1 = AgentInfo(
        agent_id=f"agent_1",
        address="localhost:50051",
        name="example agent 1",
        domain="debug",
        input_mode=[Mode.TEXT],
        output_mode=[Mode.TEXT],
        description="Hello world",
        skills=[AgentSkill(skill_id="0", capability="send text")],
        version="0.1"
    )
    
    agent_info_2 = AgentInfo(
            agent_id=f"agent_2",
            address="localhost:50052",
            name="example agent 2",
            domain="debug",
            input_mode=[Mode.TEXT],
            output_mode=[Mode.TEXT],
            description="Hello world",
            skills=[AgentSkill(skill_id="0", capability="send text")],
            version="0.1"
        )

    example_agent_server_1 = AgentServer(agent_info_1, process_request_func)
    example_agent_server_2 = AgentServer(agent_info_2, process_request_func)
    await example_agent_server_1.start()
    print('Init Agent Server 1 Done.')
    await example_agent_server_2.start()
    print('Init Agent Server 2 Done.')


    # Gateway start
    local_gw = Gateway(address="localhost:50055", gateway_id="gw_local")
    await local_gw.start()

    await example_agent_server_1.connect_to_gateway("localhost:50055")
    await example_agent_server_2.connect_to_gateway("localhost:50055")
    print('Connected to Gateway!')

    # Test Gateway Registration
    # registered_agents = await local_gw.get_registered_agents()
    # print(f"Registered Agents: {registered_agents}")
    # registered_nodes = await local_gw.get_registered_nodes()
    # print(f"Registered Nodes: {registered_nodes}")

    async def process_response_func(message: AgentMessage):
        print(f"Received Response: {message.content[0]._text}")
        if message.session_status == SessionStatus.STOP_RESPONSE:
            print("Session Stopped")

    from grpc_service import AgentServiceStub, GatewayServiceStub
    example_agent_client = AgentClient(process_response_func)
    try:
        await example_agent_client.start("localhost:50055", GatewayServiceStub, "RouteAgentCalling")
        print('Init Agent Client Done.')

        citem = ContentItem()
        citem._text = "Hello World"
        citem_1 = ContentItem()
        citem_1._text = "Hold request"
        citem_2 = ContentItem()
        citem_2._text = "Stop request"
        example_msg = AgentMessage(content=[citem],
                                   sender_id="agent_1",
                                   receiver_id="agent_2",
                                   session_id="",
                                   session_status=SessionStatus.START_QUEST,
                                   task_info=TaskInfo(task_id='0', parent_task_ids=['0'], task_status=TaskStatus.CREATE),
                                   message_id='m0',
                                   reply_to_message_id='m0')
        # await local_gw.deregister_node("agent_2")
        example_msg_1 = AgentMessage(content=[citem_1],
                                   sender_id="agent_1",
                                   receiver_id="agent_2",
                                   session_id="",
                                   session_status=SessionStatus.HOLD_QUEST,
                                   task_info=TaskInfo(task_id='0', parent_task_ids=['0'], task_status=TaskStatus.EXECUTING),
                                   message_id='m0',
                                   reply_to_message_id='m0')
        example_msg_2 = AgentMessage(content=[citem_2],
                                   sender_id="agent_1",
                                   receiver_id="agent_2",
                                   session_id="",
                                   session_status=SessionStatus.STOP_QUEST,
                                   task_info=TaskInfo(task_id='0', parent_task_ids=['0'], task_status=TaskStatus.EXECUTING),
                                   message_id='m0',
                                   reply_to_message_id='m0')
        await example_agent_client.send_message(example_msg)
        await example_agent_client.send_message(example_msg_1)
        await example_agent_client.send_message(example_msg_2)

        final_response = await example_agent_client.wait_completion()
        print(f"Final response: {final_response.content}")
    finally:
        await example_agent_client.close()


if __name__ == "__main__":
    # run main process
    asyncio.get_event_loop().set_debug(True)
    asyncio.run(main())