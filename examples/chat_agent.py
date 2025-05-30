import asyncio
import argparse
import sys
from functools import partial
from atlink_aip.grpc_service.type import AgentMessage, SessionStatus, Mode
from atlink_aip.module import Agent


async def delayed_process_request_func(message: AgentMessage, delay: float) -> AgentMessage:
    print(f"\033[33m[RESPONSE] <{message.receiver_id}> --> "
          f"<{message.sender_id}>\033[0m: {message.content[0]._text}")
    await asyncio.sleep(delay)
    text = f"Response to: {message.content[0]._text}"
    return text


async def read_input(prompt: str) -> str:
    print(prompt, end='', flush=True)
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    loop = asyncio.get_event_loop()
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)
    return (await reader.readline()).decode().strip()


async def interactive_chat_loop(agent: Agent):
    try:
        while True:
            target_id = (await read_input("\nEnter target agent id (or 'exit'): ")).strip()
            if target_id.lower() == "exit":
                break

            received_id = target_id
            session_id = await agent.create_agent_client(received_id)
            text = await read_input("Enter message to send: ")
            text = text.strip()

            await agent.submit_inquiry(
                session_id=session_id,
                receiver_id=received_id,
                content=text
            )
            print(f"\033[31m[SEND] <{agent.agent_id}> --> <{received_id}>\033[0m: {text}")
            await agent.submit_inquiry(
                session_id=session_id,
                receiver_id=received_id,
                content="Finish Talk",
                session_status=SessionStatus.STOP_QUEST
            )

            result = ""
            while True:
                response = await agent.receive_feedback(session_id, received_id)
                print(f"\033[32m[SUCCESS] Response received from <{response.sender_id}>\033[0m")
                if response.session_status == SessionStatus.STOP_RESPONSE:
                    break
                for content_item in response.content:
                    if content_item._text:
                        result += content_item._text
                await asyncio.sleep(1)
            print(f"\033[34m[RESPONSE]\033[0m:{result}")

    except asyncio.CancelledError:
        pass


async def process_server_message(agent):
    try:
        while True:
            request = await agent.receive_inquiry()
            session_id = request.session_id
            handlers = [
                partial(delayed_process_request_func, message=request, delay=2)
            ]
            results = await agent.invoke_session_handlers(session_id, handlers)
            for text in results:
                await  agent.submit_feedback(
                    session_id=request.session_id,
                    receiver_id=request.sender_id,
                    request_session_status=request.session_status,
                    content=text,
                    content_mode=[Mode.TEXT]
                )
    except asyncio.CancelledError:
        pass

async def main(agent_id: str, agent_address: str, gateway_address: str):
    agent = Agent(
        agent_id=agent_id,
        address=agent_address,
        name=f"Agent {agent_id}",
        description="Chatbot agent"
    )

    await agent.start()
    await agent.register_to_gateway(gateway_address)

    print(f"\n\033[36mAgent {agent_id} is ready at {agent_address}. Start chatting!\033[0m", flush=True)

    server_task = asyncio.create_task(process_server_message(agent))
    client_task = asyncio.create_task(interactive_chat_loop(agent))

    try:
        done, pending = await asyncio.wait(
            [server_task, client_task],
            return_when=asyncio.FIRST_COMPLETED
        )

        for task in pending:
            task.cancel()
            
    except asyncio.CancelledError:
        print("\n\033[34mTask cancelled. Stopping agent...\033[0m")
        await agent.stop()
    finally:
        await agent.stop()
        print(f"\033[35mAgent {agent_id} stopped.\033[0m")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--agent_id", type=str, required=True, help="Create an agent ID")
    parser.add_argument("--agent_address", type=str, default="localhost:50051")
    parser.add_argument("--gateway_address", default="localhost:50050")

    args = parser.parse_args()

    asyncio.run(main(agent_id=args.agent_id, agent_address=args.agent_address, gateway_address=args.gateway_address))
