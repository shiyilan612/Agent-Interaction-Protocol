import json
import argparse
import asyncio
from atlink_aip.module.client import GatewayClient
from atlink_aip.grpc_service.type import AgentInfo, ToolBoxInfo, SessionStatus, ToolResponse

async def main(args):
    client = None
    try:
        security_config = None
        if args.with_auth:
            security_config = {
                "private_key_path": args.private_key,
                "public_key_path": args.public_key
            }

        print(f"create a gateway client: {args.client_id}")
        client = GatewayClient(args.gateway_addr)
        
        if args.with_auth:
            print("Enabling secure connection...")
        
        print("Starting gateway client connection...")
        await client.start()
        print("Connected to gateway server")
        
        if not hasattr(client, '_stub') or client._stub is None:
            raise RuntimeError("Gateway connection not properly established")
        else:
            print("Gateway connection stub verified")
        
        print("Retrieving gateway nodes")
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

        print("Testing gateway tool")
        toolbox_id = "AIPTool001"
        await client.create_tool_client(toolbox_id=toolbox_id)
        response = await client.send_tool_request(
            sender_id=args.client_id,
            receiver_id=toolbox_id,
            tool_name="calculate_sum",
            arguments=json.dumps({"a": 111, "b": 333})
        )

        print(f"Tool response status: {response.status}")
        if response.status == ToolResponse.Status.SUCCESS:
            print(f"Tool execution result: {response.result}")
        else:
            print(f"Tool execution error: {response.error_message}")
            await client.close_tool_client(toolbox_id=toolbox_id)
            print(response)

        print("Testing gateway agent")
        text = "Hello World"
        agent_id = "AIPAgent001"
        session_id = await client.create_agent_client(agent_id)

        await client.submit_agent_inquiry(
            sender_id=args.client_id,
            receiver_id=agent_id,
            session_id=session_id,
            content=text
        )

        print("Waiting for agent response...")
        result = ""
        while True:
            response = await client.receive_agent_feedback(session_id, receiver_id=agent_id)
            print(f"{response.sender_id} response: {response.content}")
            
            for content_item in response.content:
                if content_item.WhichOneof('content') == 'text':
                    result += content_item.text
            
            if response.session_status == SessionStatus.STOP_RESPONSE:
                print("Agent ended response")
                break

        print(f"Final response content: {result}")

        await client.submit_agent_inquiry(
            sender_id=args.client_id,
            receiver_id=agent_id,
            session_id=session_id,
            content="End session",
            session_status=SessionStatus.STOP_QUEST
        )
        
    except Exception as e:
        print(f"Unhandled exception occurred: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        if client is not None:
            try:
                print("Closing gateway client connection...")
                if hasattr(client, 'stop'):
                    await client.stop()
                else:
                    if hasattr(client, '_tool_clients'):
                        for toolbox_id in list(client._tool_clients.keys()):
                            await client.close_tool_client(toolbox_id)
                    if hasattr(client, '_agent_clients'):
                        for session_id, receiver_id in list(client._agent_clients.keys()):
                            pass
                print("Gateway client closed")
            except Exception as stop_e:
                print(f"Error closing gateway client: {str(stop_e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway_addr", default="localhost:50001")
    parser.add_argument("--client_id", default="test_gw_client")
    parser.add_argument("--with_auth", action="store_true", help="Enable secure connection")
    parser.add_argument("--private_key", default="client.key", help="Client private key path")
    parser.add_argument("--public_key", default="ca.key", help="Root public key path")
    args = parser.parse_args()

    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        print("Program interrupted by user")