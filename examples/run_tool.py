import asyncio
import argparse
from atlink.module import Tool


async def calculate_sum(a: int, b: int) -> int:
    """Adds two numbers and returns the sum."""
    return int(a) + int(b)

async def main(gateway_address):
    #create API tool
    siRNA_tool = Tool.create_api_tool(
        address="localhost:50061",
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
    await siRNA_tool.register_to_gateway(gateway_address)
    print("API Tool registered")
    await asyncio.sleep(1)

    #create Func tool
    sum_tool = Tool.create_function_tool(
        address="localhost:50062",
        function=calculate_sum,
        name="Sum",
        tool_id="tool2",
        description="A simple tool that adds two numbers"
    )

    await sum_tool.start()
    await sum_tool.register_to_gateway(gateway_address)
    print("Func Tool registered")

    try:
        while True:
            await asyncio.sleep(9999999)
    except asyncio.CancelledError:
        print("Stopping tools...")
        await siRNA_tool.stop()
        await sum_tool.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway_address", default="localhost:50050")
    args = parser.parse_args()

    asyncio.get_event_loop().set_debug(True)
    asyncio.run(main(gateway_address=args.gateway_address))