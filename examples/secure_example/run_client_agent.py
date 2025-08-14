import json
import argparse
import asyncio
from ..module import Agent
from ..grpc_service.type import AgentInfo, ToolBoxInfo, SessionStatus

async def run_example_operations(agent):
    #获取网关中的节点信息，列出所有可访问的工具和代理
    await agent.update_peers()#从网关获取所有节点
    gateway_peers = await agent.get_nodes()
    for peer in gateway_peers.values():
        #打印工具箱信息
        if isinstance(peer, ToolBoxInfo):
            print(f"id: {peer.toolbox_id}")
            print(f"name: {peer.name}")
            print(f"domain: {peer.domain}")
            print(f"description: {peer.description}")
            print("Tools: ")
            for tool in peer.tools:
                print(f"     Name:{tool.name} | Description:{tool.description} | Arguments:{tool.arguments}")
        #打印其他代理信息
        elif isinstance(peer, AgentInfo):
            if peer.agent_id == agent.agent_id:
                continue

            print(f"id: {peer.agent_id}")
            print(f"name: {peer.name}")
            print(f"domain: {peer.domain}")
            print(f"description: {peer.description}")
        print("")

    #通过网关调用示例工具
    toolbox_id = "example_tool"
    #创建工具客户端
    await agent.create_tool_client(receiver_id=toolbox_id)
    #调用工具方法
    response = await agent.call_tool(
        toolbox_id="example_tool",
        tool_name="calculate_sum",
        arguments=json.dumps({"a": 111, "b": 333})
    )
    #关闭工具客户端
    await agent.close_tool_client(toolbox_id=toolbox_id)
    print(f"Results from example_tool: {response}")
    print("")

    #通过网关调用示例代理
    agent_id = "example_agent"
    #创建代理客户端
    session_id = await agent.create_agent_client(agent_id)
    #发送询问消息
    await agent.submit_inquiry(
        session_id=session_id,
        receiver_id=agent_id,
        content="Who are you?"
    )
    #接收代理的反馈
    await agent.submit_inquiry(
        session_id=session_id,
        receiver_id=agent_id,
        content="GoodBye!",
        session_status=SessionStatus.STOP_QUEST
    )
    #接受回复
    while True:
        response = await agent.receive_feedback(session_id, receiver_id=agent_id)

        result = ""
        for content_item in response.content:
            if content_item._text:
                result += content_item._text
        print(f"Response received from <{response.sender_id}>: {result}")

        if response.session_status == SessionStatus.STOP_RESPONSE:
            break

async def main(args):
    agent = None
    try:
        agent = Agent(
            agent_id=args.agent_id,
            host_address=args.host_address,
            name=args.agent_name,
            description="",
        )
        
        print(f"创建代理: {agent.agent_id}")
        
        if args.with_auth:
            print("启用安全连接...")
            await agent.enable_security(
                private_key_path=args.private_key,
                public_key_path=args.public_key,
            )
        
        print("启动服务器...")
        await agent.start()
        print("服务器已启动")
        
        print(f"注册到网关: {args.gateway_address}")
        await agent.register_to_gateway(args.gateway_address)
        print("已注册到网关")
        
        # 运行示例操作
        await run_example_operations(agent)
        
    except Exception as e:
        print(f"发生未捕获的异常: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        # 确保无论是否发生异常都正确关闭代理
        if agent is not None:
            try:
                print("关闭代理...")
                await agent.stop()
                print("代理已关闭")
            except Exception as stop_e:
                print(f"关闭代理时出错: {str(stop_e)}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--agent_id", default="agent client")
    parser.add_argument("--agent_name", default="Dummy Agent")
    parser.add_argument("--gateway_address", default="localhost:50000")
    parser.add_argument("--host_address", default="localhost:53000")
    parser.add_argument("--with_auth", action="store_true", help="启用安全连接")
    parser.add_argument("--private_key", default="client.key", help="客户端私钥路径")
    parser.add_argument("--public_key", default="ca.key", help="根公钥路径")
    args = parser.parse_args()

    asyncio.run(main(args))
