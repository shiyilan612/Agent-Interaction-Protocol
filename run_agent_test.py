import uuid
import asyncio

from grpc_service.type import AgentMessage, AgentInfo, TaskInfo, AgentSkill, SessionStatus, TaskStatus, Mode
from module.client import  AgentClient
from module.server import AgentServer


async def main():
    async def process_request_func(message: AgentMessage) -> AgentMessage:
        receiver_id = message.receiver_id
        sender_id = message.sender_id
        content = message.content
        print(f"<{receiver_id}>: receive \"{content}\" from {sender_id} in session: {message.session_id}")

        message.content = f"Response: {content}"
        message.sender_id = receiver_id
        message.receiver_id = sender_id
        message.task_info.task_status = TaskStatus.FINISH
        message.session_status = SessionStatus.STOP_RESPONSE

        return message

    agent_info = AgentInfo(
        agent_id=f"agent_{str(uuid.uuid4())}",
        address="localhost:50051",
        name="example agent",
        domain="debug",
        input_mode=Mode.TEXT,
        output_mode=Mode.TEXT,
        description="Hello world",
        skills=[AgentSkill(skill_id="0", capability="send text")],
        version="0.1"
    )
    example_agent_server = AgentServer(agent_info, process_request_func)
    await example_agent_server.start()
    print('Init Agent Server Done.')

    async def process_response_func(message: AgentMessage):
        print(f"Received Response: {message.content}")
        if message.session_status == SessionStatus.STOP_RESPONSE:
            print("Session Stopped")

    from grpc_service import AgentServiceStub
    example_agent_client = AgentClient(process_response_func)
    try:
        await example_agent_client.start("localhost:50051", AgentServiceStub, "CallAgent")
        print('Init Agent Client Done.')

        example_msg = AgentMessage(content="Hello World",
                                   sender_id="agent1",
                                   receiver_id="agent1",
                                   session_id="",
                                   session_status=SessionStatus.START_QUEST,
                                   task_info=TaskInfo(task_id='0', parent_task_ids=['0'], task_status=TaskStatus.CREATE),
                                   content_mode=Mode.TEXT,
                                   message_id='m0',
                                   reply_to_message_id='m0')
        await example_agent_client.send_message(example_msg)

        final_response = await example_agent_client.wait_completion()
        print(f"Final response: {final_response.content}")
    finally:
        await example_agent_client.close()


if __name__ == "__main__":
    # run main process
    asyncio.get_event_loop().set_debug(True)
    asyncio.run(main())