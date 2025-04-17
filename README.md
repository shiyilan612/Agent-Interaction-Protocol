# ACP Python SDK

## Overview

### Architecture
![alt text](asset/arch.png)


## Setup
```
git clone https://gitee.com/haixinwa/acp.git
cd ./acp
pip install -r requirements.txt
python -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. grpc_service/schema.proto
```

## Simple Test

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
python run_test.py
```