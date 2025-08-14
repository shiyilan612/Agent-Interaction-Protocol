import asyncio
import argparse
from ..module import Gateway

async def main(args):
    gateway = Gateway(
        host_address=args.host_address,
        gateway_id=args.gateway_id,
    )
    
    # 启动gateway
    await gateway.start()
    #添加存根验证方法
    """
    如果是 Gateway 对象：
        检查是否存在 _host 属性,确认 _host 的值不是 None
    如果是其他对象（如工具箱、代理等）：
        检查是否存在 _stub 属性,确认 _stub 的值不是 None
    """
    obj = gateway  # 当前要验证的对象
    if isinstance(obj, Gateway):
        # Gateway使用_host属性
        def verify_connection(self):
            return hasattr(self, '_host') and self._host is not None
        Gateway.verify_connection = verify_connection
    else:
        # 其他模块使用_stub属性
        def verify_connection(self):
            return hasattr(self, '_stub') and self._stub is not None
        obj.__class__.verify_connection = verify_connection
    
    # 然后验证连接
    if not obj.verify_connection():
        raise RuntimeError(f"{type(obj).__name__}连接初始化失败")
    """
    当命令行指定 with_auth 时，启用安全传输。
    使用私钥（server.key）和公钥（ca.key）进行加密通信。
    """
    if args.with_auth:
        await gateway.enable_security(
            private_key_path=args.private_key,
            public_key_path=args.public_key
        )
    """
    float('inf') 使程序无限期运行。
    收到 CancelledError（如 Ctrl+C）时，调用 gateway.stop() 清理资源。
    """
    try:
        await asyncio.sleep(float('inf'))
    except asyncio.CancelledError:
        await gateway.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--with_auth", action ="store_true", help="启用安全路径")
    parser.add_argument("--private_key", default="server.key", help="私钥路径")
    parser.add_argument("--public_key", default="ca.key", help="公钥路径")
    parser.add_argument("--host_address", default="localhost:50000")
    parser.add_argument("--gateway_id", default="example_gateway")
    args = parser.parse_args()

    asyncio.run(main(args))
