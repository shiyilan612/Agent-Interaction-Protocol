import asyncio
import argparse
from atlink_aip.module import Gateway

async def main(args):
    gateway = Gateway(
        host_address=args.host_address,
        gateway_id=args.gateway_id,
    )
    
    await gateway.start()
    
    obj = gateway
    if isinstance(obj, Gateway):
        def verify_connection(self):
            return hasattr(self, '_host') and self._host is not None
        Gateway.verify_connection = verify_connection
    else:
        def verify_connection(self):
            return hasattr(self, '_stub') and self._stub is not None
        obj.__class__.verify_connection = verify_connection
    
    if not obj.verify_connection():
        raise RuntimeError(f"{type(obj).__name__} connection initialization failed")
    
    if args.with_auth:
        await gateway.enable_security(
            private_key_path=args.private_key,
            public_key_path=args.public_key
        )
    
    try:
        await asyncio.sleep(float('inf'))
    except asyncio.CancelledError:
        await gateway.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--with_auth", action ="store_true", help="Enable secure path")
    parser.add_argument("--private_key", default="server.key", help="Private key path")
    parser.add_argument("--public_key", default="ca.key", help="Public key path")
    parser.add_argument("--host_address", default="localhost:50000")
    parser.add_argument("--gateway_id", default="example_gateway")
    args = parser.parse_args()

    asyncio.run(main(args))