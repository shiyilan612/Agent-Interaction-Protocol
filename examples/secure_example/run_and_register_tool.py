import asyncio
import argparse
from atlink_aip.module import ToolBox

async def main(args):
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
    
    await toolbox.start()
    
    def verify_connection(self):
        return hasattr(self, '_server') and self._server is not None
    
    ToolBox.verify_connection = verify_connection
    
    if not toolbox.verify_connection():
        server_status = "present" if hasattr(toolbox, '_server') else "absent"
        server_value = getattr(toolbox, '_server', None)
        details = f"_server attribute: {server_status}, value: {server_value}"
        raise RuntimeError(f"Toolbox connection initialization failed ({details})")
    
    @toolbox.tool()
    async def calculate_sum(a: int, b: int) -> int:
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
    parser.add_argument("--private_key", default="server.key", help="Server private key path") 
    parser.add_argument("--public_key", default="ca.key", help="Root public key path")

    args = parser.parse_args()

    asyncio.run(main(args))