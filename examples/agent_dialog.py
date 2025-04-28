"""
       +-------------+
       |   Gateway   |
       | (50050)     |
       +------+------+
              ▲
              | 路由转发
              ▼
+-----------------------------+
|  Agent2        User         |
|  Server:50052  Server:50053 |
|  Agent         Agent        |
+-----------------------------+
"""
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import asyncio
from grpc_service.type import AgentMessage, ToolRequest, ToolResponse, SessionStatus, ContentItem
from module import Agent

import re

# Handler for Assistant Agent's requests
async def assistant_request_handler(message: AgentMessage) -> AgentMessage:
    txt = message.content[0]._text
    print(f"<Assistant> Assistant received: {txt}")
    
    match = re.search(r'\d+', txt)
    if match:
        first_num = int(match.group())
    
    match = re.search(r'\d+$', txt)
    if match:
        second_num = int(match.group())
        
    res = first_num + second_num
    
    # Process the request and generate a response
    response_content = f"<Assistant> The result is: '{res}'"
    citem = ContentItem.write_text(response_content)
    
    session_status = SessionStatus.STOP_RESPONSE if message.session_status == SessionStatus.STOP_QUEST else SessionStatus.HOLD_RESPONSE
    
    #citem._text = response_content
    m = AgentMessage(
        sender_id=message.receiver_id,
        receiver_id=message.sender_id,
        content=[citem],
        session_id=message.session_id,
        session_status = session_status,
        task_info = message.task_info,
        message_id = message.message_id,
        reply_to_message_id = message.reply_to_message_id
    )
    print(m.content[0]._text)
    return m

mid_res = 0

# Handler for User Agent's responses
async def user_response_handler(message: AgentMessage) -> None:
    print(f"<User> User received response: {message.content[0]._text}")
    
    match = re.search(r'\d+', message.content[0]._text)
    if match:
        global mid_res
        mid_res = int(match.group())
    
    # print(message.session_status)
    if message.session_status == SessionStatus.STOP_RESPONSE:
        print("<User> Conversation complete.")


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
        content="Add 1 to 0"
    )
    # client.send_message()
    print("<User> Message sent to assistant: Add 1 to 0")
    
    global mid_res
    while mid_res < 5:
        await asyncio.sleep(0.5)
        session_status = SessionStatus.STOP_QUEST if mid_res == 4 else SessionStatus.HOLD_QUEST
        client = await user.send_message(
            receiver_id="agent2",
            content=f"Add 1 to {mid_res}",
            session_status=session_status
        )
        print(f"<User> Message sent to assistant: Add 1 to {mid_res}")
    
    # Wait for the session to complete

    final_response = await client.wait_completion()
    print(final_response.content[0]._text)
    
    # Clean up
    await user.stop()
    await assistant.stop()
    print("All agents stopped")


if __name__ == "__main__":
    # run main process
    asyncio.get_event_loop().set_debug(True)
    asyncio.run(main())
