import asyncio
import argparse
from functools import partial
from atlink.grpc_service.type import AgentMessage, SessionStatus, ContentItem, Mode
from atlink.module import Agent


async def delayed_process_request_func(message: AgentMessage, delay: float) -> AgentMessage:
    print(f"\033[33m[RESPONSE] <{message.receiver_id}> --> "
          f"<{message.sender_id}>\033[0m: {message.content[0]._text}")
    await asyncio.sleep(delay)
    text = f"Response to: {message.content[0]._text}"
    return text


async def read_input(prompt: str) -> str:
    return await asyncio.to_thread(input, prompt)


async def interactive_chat_loop(agent: Agent):
    try:
        while True:
            target_num = (await read_input("\nEnter target agent number (or 'exit'): ")).strip()
            if target_num.lower() == "exit":
                exit(0)
            if not target_num.isdigit():
                print("Invalid input. Please enter a number.")
                continue

            received_id = f"agent{target_num}"
            session_id = await agent.create_agent_client(received_id)
            text = await read_input("Enter message to send: ")
            text = text.strip()

            await agent.submit_inquiry(
                session_id=session_id,
                receiver_id=received_id,
                content=text
            )
            print(f"\033[31m[SEND] <{agent.agent_id}> --> <{received_id}>\033[0m: {text}")

            while True:
                response = await agent.receive_feedback(session_id, received_id)
                print(f"\033[32m[SUCCESS] Response received from <{response.sender_id}>\033[0m")
                if response.session_status == SessionStatus.STOP_RESPONSE:
                    break
                await asyncio.sleep(1)

    except asyncio.CancelledError:
        print("\n\033[34mInput loop cancelled. Exiting...\033[0m")
    except KeyboardInterrupt:
        print("\n\033[34mInterrupted by user. Exiting...\033[0m")


async def process_server_message(agent):
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
                content=text,
                content_mode=[Mode.TEXT],
                session_status=SessionStatus.STOP_RESPONSE
            )

async def main(agent_num: int, gateway_address: str):
    agent_id = f"agent{agent_num}"
    address = f"localhost:5100{agent_num}"

    agent = Agent(
        agent_id=agent_id,
        address=address,
        name=f"Agent {agent_id}",
        description="Chatbot agent"
    )

    await agent.start()
    await agent.register_to_gateway(gateway_address)

    print(f"\n\033[36mAgent {agent_id} is ready at {address}. Start chatting!\033[0m")

    server_task = asyncio.create_task(process_server_message(agent))
    client_task = asyncio.create_task(interactive_chat_loop(agent))

    try:
        await asyncio.gather(server_task, client_task)
    finally:
        await agent.stop()
        print(f"\033[35mAgent {agent_id} stopped.\033[0m")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--agent-num", type=int, required=True, help="Numeric agent ID, e.g. 1 for agent1")
    parser.add_argument("--gateway-address", default="localhost:50050")

    args = parser.parse_args()

    asyncio.run(main(agent_num=args.agent_num, gateway_address=args.gateway_address))
