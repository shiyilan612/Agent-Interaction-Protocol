import asyncio
import argparse
from atlink_aip.module import Tool, ToolBox


async def main(gateway_address):
    #create API tool
    siRNA_tool = Tool.create_api_tool(
        api_url="https://siRNA_tool_url",
        api_method="POST",
        api_timeout=100,
        name="小核酸siRNA效力预测",
        description="用于小核酸预测siRNA对给定mRNA序列的抑制效果的工具",
        arguments={
            "mRNA": "参数是一个列表，列表中的每个元素代表mRNA序列，将被分析以设计 siRNA",
            "siRNA": "可选的 siRNA 序列列表，用于评估特定 siRNA 的效果",
            "config": "配置参数对象，包含 top_n, no_func, off_target, toxicity, all_human 等选项"
        }
    )
    
    toolbox = ToolBox(address="localhost:50061", toolbox_id="toolbox1", name="ToolBox1", tools=[siRNA_tool])

    #create Func tool
    @toolbox.tool(name="Sum")
    async def calculate_sum(a: int, b: int) -> int:
        """Adds two numbers and returns the sum."""
        return int(a) + int(b)

    await toolbox.start()
    await toolbox.register_to_gateway(gateway_address)
    print("toolbox registered")

    try:
        while True:
            await asyncio.sleep(9999999)
    except asyncio.CancelledError:
        print("Stopping tools...")
        await toolbox.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway_address", default="localhost:50050")
    args = parser.parse_args()

    asyncio.get_event_loop().set_debug(True)
    asyncio.run(main(gateway_address=args.gateway_address))