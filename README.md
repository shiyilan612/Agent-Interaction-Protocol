# ATLink Python SDK

## Overview

### Architecture
<p align="left"><img src="asset/arch.png" width = "350" height = "300"></p>

## Setup
```
git clone https://gitee.com/haixinwa/AgentLinkProtocol.git
cd ./AgentLinkProtocol
pip install -r requirements.txt
python -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. atlink/grpc_service/schema.proto
```

## Simple Service Test

```
消息流转示意图：
Agent A                   Gateway                 Agent B
   |------发送到B的消息------>|                       |
   |                         |----建立到B的连接------->|
   |                         |----转发消息----------->|
   |                         |<--------响应----------|
   |<--------响应 -----------|                       |
```

```
python run_service_test.py
```