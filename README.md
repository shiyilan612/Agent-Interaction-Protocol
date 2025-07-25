# AIP Python SDK

  <p>
      <a href="README.md">English</a> | <a href="README_zh.md">简体中文</a> 
  </p>

## 📋 Introduction
**AIP** (Agent Interaction Protocol) is a distributed agent interaction protocol developed by the Institute of Automation, Chinese Academy of Sciences.

It defines communication mechanisms for multi-agent collaboration, multi-tool invocation, and multi-modal data access in scientific scenarios. AIP also provides functional components to support the rapid development of large-scale agent-based scientific systems.

### ✨ Features
- **gRPC Communication**: Using gRPC as the underlying communication framework, data is transmitted in binary format. Compared to JSON-RPC, it is more lightweight and offers faster serialization/deserialization performance.

- **Routing Mode**: In addition to common point-to-point communication, AIP supports a gateway-centered routing mode, making it easier to manage scientific agent groups.

- **Unified Interface**: Supports both agent-to-agent bidirectional streaming interaction and agent-to-tool/data access.

- **MCP Compatible**: Defines an MCP proxy node that enables direct integration of MCP services into systems built on AIP, allowing unified management and invocation through a gateway without modifying the protocol format.

### Framework
<p align="left"><img src="asset/arch.png" width = "600" height = "300"></p>

## 📦 Installation
```bash
git clone https://github.com/ScienceOne-AI/Agent-Interaction-Protocol.git
cd ./Agent-Interaction-Protocol
pip install .
```

## 🚀 Quick Start Examples

Some simple examples are in the directory `./example `. You can follow these steps to conduct the test.

### 1. Run gateway
```bash
python run_gateway.py
```

```python
# run_gateway.py
from atlink_aip.module import Gateway

# set address, ID of the gateway
gateway = Gateway(host_address=args.host_address, gateway_id=args.gateway_id)

# start gateway
await gateway.start()
```

### 2. Run and register Tool
```bash
python run_and_register_tool.py
```

```python
# run_and_register_tool.py
from atlink_aip.module import ToolBox

# set address, name and ID of the toolbox
toolbox = ToolBox(host_address=args.host_address, name=args.toolbox_name, toolbox_id=args.toolbox_id)

# add a tool
@toolbox.tool()
async def calculate_sum(a: int, b: int) -> int:
    """Adds two numbers and returns the sum."""
    return int(a) + int(b)

# register toolbox to gateway
await toolbox.start()
await toolbox.register_to_gateway(args.gateway_address)
```

### 3. Run and register agent
```bash
pip install requests # install `requests` to call LLM API
python run_and_register_agent.py --llm_url "api url of a LLM" --api_key "api key of a LLM" --model "LLM name"
```

```python
# run_and_register_agent.py
from atlink_aip.grpc_service.type import Mode
from atlink_aip.module import Agent

# set address, name and ID of the agent
agent = Agent(
    agent_id=args.agent_id,
    host_address=args.host_address,
    name=args.agent_name,
    description=f"LLM Mode:{args.model}"
)

# register agent to gateway
await agent.start()
await agent.register_to_gateway(args.gateway_address)
asyncio.create_task(process_message(agent, args))
```

```python
# call LLM API to process AIP message
async def process_message(agent, args):
    async def call_LLM(url: str, api_key: str, model: str, text: str) -> str:
        headers = {
            "Authorization": api_key,
            "Content-Type": "application/json"
        }
        system_prompt = "You are an agent that communicates using the AIP protocol."
        user_prompt = f"{text}"
        params = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "stream": False
        }
        try:
            response = requests.post(url, json=params, headers=headers)
            response.raise_for_status()
            result = response.json()
            output = result['choices'][0]['message']['content']
            return output

        except Exception as e:
            return f"{str(e)}"

    try:
        while True:
            request = await agent.receive_inquiry()
            session_id = request.session_id
            handlers = [
                partial(
                    call_LLM,
                    url=args.llm_url,
                    api_key=args.api_key,
                    model=args.model,
                    text=request.content[0]._text
                )
            ]
            # After receiving the message, the agent will invoke the ‘call_LLM’ method.
            results = await agent.invoke_session_handlers(session_id, handlers)
            for text in results:
                await  agent.submit_feedback(
                    session_id=request.session_id,
                    receiver_id=request.sender_id,
                    request_session_status=request.session_status,
                    content=text,
                    content_mode=[Mode.TEXT]
                )
    except:
        await agent.stop()
```

### 4. Access the tool and the agent through a client agent 
```bash
python run_client_agent.py
```

```python
# run_client_agent.py

# invoke the example tool through the gateway.
toolbox_id = "example_tool"
await agent.create_tool_client(receiver_id=toolbox_id)  # create a tool client.
response = await agent.call_tool(
    toolbox_id="example_tool",
    tool_name="calculate_sum",
    arguments=json.dumps({"a": 111, "b": 333})
)
await agent.close_tool_client(toolbox_id=toolbox_id) # close the Tool Client
print(f"Results from example_tool: {response}")

# invoke the example agent through the gateway.
agent_id = "example_agent"
session_id = await agent.create_agent_client(agent_id) # create an agent client. The agent client does not need to be manually closed.
await agent.submit_inquiry(
    session_id=session_id,
    receiver_id=agent_id,
    content="Who are you?"
)
await agent.submit_inquiry(
    session_id=session_id,
    receiver_id=agent_id,
    content="GoodBye!",
    session_status=SessionStatus.STOP_QUEST # If you need to end this communication, you should set the ·SessionStatus· to ·STOP_QUEST·.
)

while True:
    response = await agent.receive_feedback(session_id, receiver_id=agent_id)
    result = ""
    for content_item in response.content:
        if content_item._text:
            result += content_item._text
    print(f"Response received from <{response.sender_id}>: {result}")

    # If the ·SessionStatus· in the received message is ·STOP_RESPONSE·, then this message is the last one of this communication.
    if response.session_status == SessionStatus.STOP_RESPONSE:
        break
```

## 🛠️ Real-World Examples

🔖 **Case**: Apply AIP to small nucleic acid siRNA efficacy analysis

<p align="left"><img src="./asset/AIP-Case.gif" alt="demo" width="600"/></p>


## ⏳ To Do
- [ ] AIP will support authentication for secure connections
- [ ] AIP will support Nodes of scientific data resource

