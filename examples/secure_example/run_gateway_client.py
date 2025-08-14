import json
import argparse
import asyncio
from ..module.client import GatewayClient
from ..grpc_service.type import AgentInfo, ToolBoxInfo, SessionStatus, ToolResponse

async def main(args):
    client = None
    try:
        security_config = None
        if args.with_auth:
            security_config = {
                "private_key_path": args.private_key,
                "public_key_path": args.public_key
            }

        print(f"创建网关客户端: {args.client_id}")
        client = GatewayClient(args.gateway_addr)
        
        # 启用安全连接
        if args.with_auth:
            print("启用安全连接...")
            # 根据实际API可能需要类似这样的调用:
            # await client.enable_security(private_key_path=args.private_key, public_key_path=args.public_key)
        
        print("启动网关客户端连接...")
        await client.start()
        print("已连接到网关服务器")
        
        # 存根验证 - 检查连接是否建立
        if not hasattr(client, '_stub') or client._stub is None:
            raise RuntimeError("网关连接未正确建立")
        else:
            print("网关连接存根验证通过")
        
        print("获取网关节点")
        gateway_peers = await client.get_gateway_nodes(sender_id=args.client_id)
        for peer in gateway_peers.values():
            if isinstance(peer, ToolBoxInfo):
                print(f"id: {peer.toolbox_id}")
                print(f"name: {peer.name}")
                print(f"domain: {peer.domain}")
                print(f"description: {peer.description}")
                print("Tools: ")
                for tool in peer.tools:
                    print(f"     Name:{tool.name} | Description:{tool.description} | Arguments:{tool.arguments}")
            elif isinstance(peer, AgentInfo):
                print(f"id: {peer.agent_id}")
                print(f"name: {peer.name}")
                print(f"domain: {peer.domain}")
                print(f"description: {peer.description}")
            print("")

        print("测试网关工具")
        toolbox_id = "AIPTool001"
        await client.create_tool_client(toolbox_id=toolbox_id)
        response = await client.send_tool_request(
            sender_id=args.client_id,
            receiver_id=toolbox_id,
            tool_name="calculate_sum",
            arguments=json.dumps({"a": 111, "b": 333})
        )

        # 添加工具响应处理
        print(f"工具响应状态: {response.status}")
        if response.status == ToolResponse.Status.SUCCESS:
            print(f"工具执行结果: {response.result}")
        else:
            print(f"工具执行错误: {response.error_message}")
            await client.close_tool_client(toolbox_id=toolbox_id)
            print(response)

        print("测试网关agent")
        text = "Hello World"
        agent_id = "AIPAgent001"
        session_id = await client.create_agent_client(agent_id)

        # 发送初始消息
        await client.submit_agent_inquiry(
            sender_id=args.client_id,
            receiver_id=agent_id,
            session_id=session_id,
            content=text
        )

        # 添加等待回复的逻辑
        print("等待Agent回复...")
        result = ""
        while True:
            response = await client.receive_agent_feedback(session_id, receiver_id=agent_id)
            print(f"{response.sender_id} 回复: {response.content}")
            
            # 收集回复内容
            for content_item in response.content:
                if content_item.WhichOneof('content') == 'text':
                    result += content_item.text
            
            # 检查会话状态
            if response.session_status == SessionStatus.STOP_RESPONSE:
                print("Agent 结束回复")
                break

        print(f"最终回复内容: {result}")

        # 结束会话
        await client.submit_agent_inquiry(
            sender_id=args.client_id,
            receiver_id=agent_id,
            session_id=session_id,
            content="结束会话",
            session_status=SessionStatus.STOP_QUEST
        )
        
    except Exception as e:
        print(f"发生未捕获的异常: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        # 确保无论是否发生异常都正确关闭客户端
        if client is not None:
            try:
                print("关闭网关客户端连接...")
                # 假设GatewayClient有stop方法
                # 如果没有，可能需要单独关闭各种连接
                if hasattr(client, 'stop'):
                    await client.stop()
                else:
                    # 尝试关闭所有活动的连接
                    if hasattr(client, '_tool_clients'):
                        for toolbox_id in list(client._tool_clients.keys()):
                            await client.close_tool_client(toolbox_id)
                    if hasattr(client, '_agent_clients'):
                        for session_id, receiver_id in list(client._agent_clients.keys()):
                            # 需要根据实际API调整
                            pass
                print("网关客户端已关闭")
            except Exception as stop_e:
                print(f"关闭网关客户端时出错: {str(stop_e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway_addr", default="localhost:50001")
    parser.add_argument("--client_id", default="test_gw_client")
    parser.add_argument("--with_auth", action="store_true", help="启用安全连接")
    parser.add_argument("--private_key", default="client.key", help="客户端私钥路径")
    parser.add_argument("--public_key", default="ca.key", help="根公钥路径")
    args = parser.parse_args()

    # 使用更简洁的asyncio.run方法
    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        print("程序被用户中断")
