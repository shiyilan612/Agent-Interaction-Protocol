# AIP Python SDK

  <p>
      <a href="README.md">English</a> | <a href="README_zh.md">简体中文</a> 
  </p>

## 📋 Introduction
**AIP** (Agent Interaction Protocol) is a distributed agent interaction protocol developed by the "AI + Science" Research Department of the Institute of Automation, Chinese Academy of Sciences.

It defines communication mechanisms for multi-agent collaboration, multi-tool invocation, and multi-modal data access in scientific scenarios. AIP also provides functional components to support the rapid development of large-scale agent-based scientific systems.

### ✨ Features
- **gRPC Communication**: Using gRPC as the underlying communication framework, data is transmitted in binary format. Compared to JSON-RPC, it is more lightweight and offers faster serialization/deserialization performance.

- **Routing Mode**: In addition to common point-to-point communication, AIP supports a gateway-centered routing mode, making it easier to manage scientific agent groups.

- **Unified Interface**: Supports both agent-to-agent bidirectional streaming interaction and agent-to-tool/data access.

### Framework
<p align="left"><img src="asset/arch.png" width = "600" height = "300"></p>

## 📦 Installation
```
git clone https://github.com/ScienceOne-AI/Agent-Interaction-Protocol.git
cd ./Agent-Interaction-Protocol
python -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. atlink_aip/grpc_service/schema.proto
pip install .
```

## 🚀 Quick Start Examples

### 1. Run Gateway
```python
# run_gateway.py
import asyncio
from atlink_aip.module import Gateway

async def main():
    # Set gateway address, ID
    gateway = Gateway(address="localhost:50050", gateway_id="test_gw")
    # Start gateway
    await gateway.start()
    print("Gateway started. Press Ctrl+C to exit.")

    try:
        while True:
            await asyncio.sleep(9999999)
    except asyncio.CancelledError:
        print("Stopping gateway...")
        await gateway.stop()

if __name__ == "__main__":
    asyncio.run(main())
```

### 2. Register Tool
```python
import asyncio
from atlink_aip.module import Tool

# Custom tool function
async def calculate_sum(a: int, b: int) -> int:
    """Adds two numbers and returns the sum."""
    return int(a) + int(b)

async def main(gateway_address):
    # Set tool address, ID and other parameters to create a function-type tool
    sum_tool = Tool.create_function_tool(
        address="localhost:50062",
        function=calculate_sum,
        name="Sum",
        tool_id="tool1",
        description="A simple tool that adds two numbers"
    )
    # Start tool
    await sum_tool.start()
    # Register tool to gateway
    await sum_tool.register_to_gateway(gateway_address)
    print("Func Tool registered")

    try:
        while True:
            await asyncio.sleep(9999999)
    except asyncio.CancelledError:
        print("Stopping tools...")
        await sum_tool.stop()

if __name__ == "__main__":
    asyncio.get_event_loop().set_debug(True)
    asyncio.run(main(gateway_address="localhost:50050"))
```

### 3. Register Agent
```python
import asyncio
import argparse
from functools import partial
from atlink_aip.grpc_service.type import AgentMessage, SessionStatus, Mode
from atlink_aip.module import Agent

# Custom agent request processing function
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
            target_id = (await read_input("\nEnter target agent id (or 'exit'): ")).strip()
            if target_id.lower() == "exit":
                exit(0)
            received_id = target_id
            # Create session
            session_id = await agent.create_agent_client(received_id)
            text = await read_input("Enter message to send: ")
            text = text.strip()
            # Send request message
            await agent.submit_inquiry(
                session_id=session_id,
                receiver_id=received_id,
                content=text
            )
            print(f"\033[31m[SEND] <{agent.agent_id}> --> <{received_id}>\033[0m: {text}")
            # Send request to end session
            await agent.submit_inquiry(
                session_id=session_id,
                receiver_id=received_id,
                content="Finish Talk",
                session_status=SessionStatus.STOP_QUEST
            )

            result = ""
            while True:
                # Receive feedback message
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
        print("\n\033[34mInput loop cancelled. Exiting...\033[0m")
    except KeyboardInterrupt:
        print("\n\033[34mInterrupted by user. Exiting...\033[0m")

# Request message processing logic
async def process_server_message(agent):
    while True:
        # Receive request
        request = await agent.receive_inquiry()
        session_id = request.session_id
        # Set request processing
        handlers = [
            partial(delayed_process_request_func, message=request, delay=2)
        ]
        # Request processing
        results = await agent.invoke_session_handlers(session_id, handlers)
        for text in results:
            # Send processing result
            await  agent.submit_feedback(
                session_id=request.session_id,
                receiver_id=request.sender_id,
                request_session_status=request.session_status,
                content=text,
                content_mode=[Mode.TEXT]
            )

async def main(agent_id: str, agent_address: str, gateway_address: str):
    # Set agent address, ID and other parameters to create agent
    agent = Agent(
        agent_id=agent_id,
        address=agent_address,
        name=f"Agent {agent_id}",
        description="Chatbot agent"
    )
    # Satrt agent
    await agent.start()
    # Register agent to gateway
    await agent.register_to_gateway(gateway_address)
    # Configure agent's request message processing logic
    server_task = asyncio.create_task(process_server_message(agent))

    # Session task example
    client_task = asyncio.create_task(interactive_chat_loop(agent))
    try:
        await asyncio.gather(server_task, client_task)
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

```

## 🛠️ Real-World Examples
Check out the [`examples/`](./examples) directory for real-world examples

🔖 **Case**: Apply AIP to small nucleic acid siRNA efficacy analysis

<p align="left"><img src="./asset/AIP-Case.gif" alt="demo" width="600"/></p>


## ⏳ To Do
- [ ] AIP will support Nodes of scientific data resource
- [ ] AIP will support rapid construction of dynamic workflows
