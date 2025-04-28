"""
       +-------------+
       |   Gateway   |
       | (50050)     |
       +------+------+
              ▲
              | 路由转发
              ▼
+---------------------------------------------------+
|  Agent2           User            SumCalculator   |
|  Server:50052     Server:50053    Server:50051    |
|  Agent            Agent           Tool            |
+---------------------------------------------------+
"""
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import asyncio
from grpc_service.type import AgentMessage, ToolRequest, ToolResponse, SessionStatus, ContentItem
from module import Agent


# Handler for Assistant Agent's requests
async def assistant_request_handler(message: AgentMessage) -> AgentMessage:
    txt = message.content[0]._text
    print(f"Assistant received: {txt}")
    
    # Process the request and generate a response
    response_content = f"I've processed your request: '{txt}'"
    citem = ContentItem.write_text(response_content)
    #citem._text = response_content
    m = AgentMessage(
        sender_id=message.receiver_id,
        receiver_id=message.sender_id,
        content=[citem],
        session_id=message.session_id,
        session_status = SessionStatus.STOP_RESPONSE,
        task_info = message.task_info,
        message_id = message.message_id,
        reply_to_message_id = message.reply_to_message_id
    )
    print(m.content[0]._text)
    return m


# Handler for User Agent's responses
async def user_response_handler(message: AgentMessage) -> None:
    print(f"User received response: {message.content[0]._text}")
    print(message.session_status)
    if message.session_status == SessionStatus.STOP_RESPONSE:
        print("Conversation complete.")


async def main():

    # Create and start Assistant Agent
    assistant = Agent(
        address="localhost:50052",
        agent_id="agent2",
        name="Assistant Agent",
        description="An agent that provides assistance"
    )
    await assistant.set_process_request_handler(assistant_request_handler).start()
    await assistant.register_to_gateway("localhost:50050")
    await asyncio.sleep(1)
    print("Assistant agent2 registered")
    
    # Create and start User Agent
    user = Agent(
        address="localhost:50053",
        agent_id="agent1",
        name="User Agent",
        description="An agent representing the user"
    )
    await user.set_process_response_handler(user_response_handler).start()
    await user.register_to_gateway("localhost:50050")
    await asyncio.sleep(1)
    print("User agent1 registered")
    
    # User sends a message to the Assistant
    client = await user.send_message(
        receiver_id="agent2",
        content="What is the result of 10 + 20?"
    )
    print("Message sent to assistant")
    
    # Wait for the session to complete

    final_response = await client.wait_completion()
    print(final_response.content[0]._text)
    
    # User calls the Calculator tool directly
    response = await user.call_tool(
        tool_id="tool1",
        tool_name="Sum",
        arguments={"a":10, "b":20},
    )
    print(f"Tool response: {response.content[0]._text}")
          
    # Clean up
    await user.stop()
    await assistant.stop()
    print("All agents stopped")


if __name__ == "__main__":
    # run main process
    asyncio.get_event_loop().set_debug(True)
    asyncio.run(main())
