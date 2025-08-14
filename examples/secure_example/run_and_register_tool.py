import asyncio
import argparse
from ..module import ToolBox

async def main(args):
    #init toolbox
    toolbox = ToolBox(
        host_address=args.host_address,
        name=args.toolbox_name,
        toolbox_id=args.toolbox_id
    )
    
    if args.with_auth:
        await toolbox.enable_security(
            private_key_path=args.private_key,
            public_key_path=args.public_key,
        )
    
    # register toolbox to gateway
    await toolbox.start()
    
    #基于实际代码的验证
    #为ToolBox类添加验证方法
    def verify_connection(self):
        """检查工具箱是否成功启动"""
        #根据实际代码，使用_server属性验证,通过验证 _server 属性是否存在且非空（该属性由 start() 初始化）。
        return hasattr(self, '_server') and self._server is not None
    
    #动态添加验证方法
    ToolBox.verify_connection = verify_connection
    
    #验证连接
    if not toolbox.verify_connection():
        #获取更详细的错误信息
        server_status = "存在" if hasattr(toolbox, '_server') else "不存在"
        server_value = getattr(toolbox, '_server', None)
        details = f"_server属性: {server_status}, 值: {server_value}"
        raise RuntimeError(f"工具箱连接初始化失败 ({details})")
    
    #add tool
    @toolbox.tool()
    async def calculate_sum(a: int, b: int) -> int:
        """Adds two numbers and returns the sum."""
        return int(a) + int(b)

    await toolbox.register_to_gateway(args.gateway_address)

    try:
        await asyncio.sleep(float('inf'))
    except asyncio.CancelledError:
        await toolbox.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway_address", default="localhost:50000")
    parser.add_argument("--host_address", default="localhost:51000")
    parser.add_argument("--toolbox_name", default="Math Box")
    parser.add_argument("--toolbox_id", default="example_tool")
    parser.add_argument("--with_auth", action="store_true")
    parser.add_argument("--private_key", default="server.key", help="服务端私钥路径") 
    parser.add_argument("--public_key", default="ca.key", help="根公钥路径")

    args = parser.parse_args()

    asyncio.run(main(args))
