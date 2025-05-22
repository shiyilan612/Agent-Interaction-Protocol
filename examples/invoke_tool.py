import asyncio
from atlink.module import Agent, Tool


async def main():
    # Create and start User Agent
    user = Agent(
        address="localhost:50053",
        agent_id="agent1",
        name="User Agent",
        description="An agent representing the user"
    )
    await user.start()
    await user.register_to_gateway("localhost:50050")
    await asyncio.sleep(1)
    print("User agent1 registered")

    # 注册 API 工具，保持简单的 arguments 定义用于描述
    siRNA_tool = Tool.create_api_tool(
        address="localhost:50051",
        api_url="https://gateway.taichuai.cn/oligo-former/infer",
        api_method="POST",
        api_timeout=100,
        name="小核酸siRNA效力预测",
        tool_id="tool1",
        description="用于小核酸预测siRNA对给定mRNA序列的抑制效果的工具",
        arguments={
            "mRNA": "mRNA 序列列表，每个序列将被分析以设计 siRNA",
            "siRNA": "可选的 siRNA 序列列表，用于评估特定 siRNA 的效果",
            "config": "配置参数对象，包含 top_n, no_func, off_target, toxicity, all_human 等选项"
        }
    )

    await siRNA_tool.start()
    await siRNA_tool.register_to_gateway("localhost:50050")
    print("Tool registered")
    await asyncio.sleep(10)

    # 现在可以传递复杂的嵌套参数结构
    response = await user.call_tool(
        tool_id="tool1",
        tool_name="小核酸siRNA效力预测",
        arguments={
            "mRNA": ["AAUCAGGUGCUUAUGAGCAUGAA"],
            "config": {
                "off_target": True,
                "toxicity": True
            }
        }
    )
    print(f"Tool response: {response.content[0]._text}")

    # Clean up
    await user.stop()
    print("All agents stopped")



asyncio.get_event_loop().set_debug(True)
asyncio.run(main())
