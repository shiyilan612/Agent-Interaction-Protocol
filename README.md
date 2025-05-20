# ATLink Python SDK

## 概览

### 框架示意图
<p align="left"><img src="asset/arch.png" width = "350" height = "300"></p>

### 1.简介
ATLink智能体互联协议是中国科学院自动化研究所“AI+科学”研究部开发的分布式智能体交互协议，面向以科学场景为代表的多智能体交互协作以及科学工具/数据调用。ATLink统一了智能体交互和智能体对工具/数据访问的接口，除了常见的点对点通信外，还支持以网关为中心的路由通信模型，填补了现有科学场景中无法良好支持大规模科学智能体联动的空白。ATLink采用gRPC作为底层通信框架，以二进制形式传输数据，比JSON RPC更加轻量，且序列化/反序列化速度更快。

### 2. 核心组件：
- 网关（Gateway）
- 节点Node（包括Agent、Tool）

### 3. 通信机制：
#### 路由（Routing）模式
- Agent <---- 双向流式 ----> Gateway <---- 双向流式 ----> Agent
- Agent <---- 双向流式 ----> Gateway ---- 单向同步请求 ----> Tool
- Gateway <-- 单向同步响应 ------ Tool

#### 点对点（N2N）模式
- Agent <---- 双向流式 ----> Agent
- Agent ---- 单向同步请求 -----> Tool
- Agent <--- 单向同步响应 ------ Tool

### 4. 消息类型：
| 消息名称       | 消息类型   | 发送方           | 接收方           | 含义                        |
|----------------|------------|------------------|---------------|-----------------------------|
| AgentMessage   | 异步流式   | Agent / Gateway  | Agent / Gateway  | 智能体代理之间的消息        |
| ToolRequest    | 同步单向   | Agent / Gateway  | Tool             | 智能体调用工具的请求        |
| ToolResponse   | 同步单向   | Tool             | Agent / Gateway  | 工具对调用请求的响应        |


## 安装
```
git clone https://gitee.com/haixinwa/AgentLinkProtocol.git
cd ./AgentLinkProtocol
pip install -e .
```

## Run Example
```
python examples/run_gateway.py
python examples/chat_agent.py
...
```

## TO DO
- [ ] 添加Resource节点
- [ ] 添加MCP Server代理节点