# AIP Python SDK

  <p>
      <a href="README.md">English</a> | <a href="README_zh.md">简体中文</a> 
  </p>

## 📋 简介
**AIP智能体交互协议** (Agent Interaction Protocol) 是中国科学院自动化研究所开发的分布式智能体交互协议，定义了面向科学场景为代表的多智能体协作、多工具调用、多模态数据访问等通信机制，并提供相关功能组件，支持大规模科学智能体联动场景的快速构建。

### ✨ 特点
- **gRPC通信**：以gRPC作为底层通信框架，以二进制形式传输数据，相比JSON RPC更加轻量，序列化/反序列化速度更快。
- **路由模式**：除常见的点对点通信外，支持以 *网关* 为中心的路由通信模式，便于科学智能体群组管理。
- **统一接口**：同时支持 *智能体-智能体* 双向流式交互和 *智能体-工具/数据* 访问
- **兼容MCP**：定义了MCP代理节点，可将MCP服务直接接入基于AIP构建的系统，通过网关进行统一的管理与调用，无需修改协议格式。

### 框架示意图
<p align="left"><img src="asset/arch.png" width = "600" height = "300"></p>

## 📦 安装
```
git clone https://github.com/ScienceOne-AI/Agent-Interaction-Protocol.git
cd ./Agent-Interaction-Protocol
pip install .
```

## 🚀 快速入门示例
一些简单的示例位于目录 `./example` 中，可以按照以下步骤进行测试：
### 1. 运行网关
```bash
python run_gateway.py
```

```python
# run_gateway.py
from atlink_aip.module import Gateway

# 设置网关地址、ID
gateway = Gateway(host_address=args.host_address, gateway_id=args.gateway_id)

# 启动网关
await gateway.start()
```

### 2. 运行工具，并注册到网关
```bash
python run_and_register_tool.py
```

```python
# run_and_register_tool.py
from atlink_aip.module import ToolBox

# 设置工具箱地址、名字、ID
toolbox = ToolBox(host_address=args.host_address, name=args.toolbox_name, toolbox_id=args.toolbox_id)

# 在工具箱中加入一个工具
@toolbox.tool()
async def calculate_sum(a: int, b: int) -> int:
    """Adds two numbers and returns the sum."""
    return int(a) + int(b)

# 启动工具箱并注册到网关
await toolbox.start()
await toolbox.register_to_gateway(args.gateway_address)
```

### 3. 运行Agent，并注册到网关
```bash
pip install requests # install `requests` to call LLM API
python run_and_register_agent.py --llm_url "api url of a LLM" --api_key "api key of a LLM" --model "LLM name"
```

```python
# run_and_register_agent.py
from atlink_aip.grpc_service.type import Mode
from atlink_aip.module import Agent

# 设置Agent地址、名字、ID
agent = Agent(
    agent_id=args.agent_id,
    host_address=args.host_address,
    name=args.agent_name,
    description=f"LLM Mode:{args.model}"
)

# 启动Agent并注册到网关
await agent.start()
await agent.register_to_gateway(args.gateway_address)
asyncio.create_task(process_message(agent, args))
```

```python
# 通过调用LLM API处理AIP消息
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
            # Agent接收到消息后会调用call_LLM方法
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

### 4. 通过客户端Agent访问网关上的工具和服务端Agent。
```bash
python run_client_agent.py
```

```python
# run_client_agent.py

# 通过网关调用示例工具
toolbox_id = "example_tool"
await agent.create_tool_client(receiver_id=toolbox_id) # 创建一个tool客户端
response = await agent.call_tool(
    toolbox_id="example_tool",
    tool_name="calculate_sum",
    arguments=json.dumps({"a": 111, "b": 333})
)
await agent.close_tool_client(toolbox_id=toolbox_id) # 主动关闭tool客户端
print(f"Results from example_tool: {response}")

# 通过网关调用示例Agent
agent_id = "example_agent"
session_id = await agent.create_agent_client(agent_id) # 创建一个agent客户端，agent客户端无需手动关闭
await agent.submit_inquiry(
    session_id=session_id,
    receiver_id=agent_id,
    content="Who are you?"
)
await agent.submit_inquiry(
    session_id=session_id,
    receiver_id=agent_id,
    content="GoodBye!",
    session_status=SessionStatus.STOP_QUEST # 若需要结束本次通信，需要将SessionStatus设置为STOP_QUEST
)

while True:
    response = await agent.receive_feedback(session_id, receiver_id=agent_id)
    result = ""
    for content_item in response.content:
        if content_item._text:
            result += content_item._text
    print(f"Response received from <{response.sender_id}>: {result}")
    
    # 若接收到消息中的SessionStatus为STOP_RESPONSE，则该消息是本次通信的最后一条消息
    if response.session_status == SessionStatus.STOP_RESPONSE:
        break
```

## 🛠️ 实际应用示例

🔖 **案例**: AIP用于小核酸siRNA效力分析

<p align="left"><img src="./asset/AIP-Case.gif" alt="demo" width="600"/></p>


## ⏳ 未来计划
- [ ] AIP 将支持安全连接的身份验证
- [ ] 支持科学数据 Resource 节点
