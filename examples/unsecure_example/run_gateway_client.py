import json
import argparse
import asyncio
from atlink_aip.module.client import GatewayClient
from atlink_aip.grpc_service.type import AgentInfo, ToolBoxInfo, SessionStatus, ToolResponse

async def main(args):
    client = None
    try:
        print(f"create a gateway client: {args.client_id}")
        client = GatewayClient(args.gateway_addr)
        
        print("start the gateway client connection...")
        await client.start()
        print("Connected to the gateway server")
        
        print("Obtain the gateway node")
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

        print("Tseting the gateway tool")
        toolbox_id = "AIPTool001"
        await client.create_tool_client(toolbox_id=toolbox_id)
        response = await client.send_tool_request(
            sender_id=args.client_id,
            receiver_id=toolbox_id,
            tool_name="calculate_sum",
            arguments=json.dumps({"a": 111, "b": 333})
        )

        print(f"tool response status: {response.status}")
        if response.status == ToolResponse.Status.SUCCESS:
            print(f"tool execution result: {response.result}")
        else:
            print(f"tool execution error: {response.error_message}")
            await client.close_tool_client(toolbox_id=toolbox_id)
            print(response)

        print("gateway_agent test")
        text = "Hello World"
        agent_id = "AIPAgent001"
        session_id = await client.create_agent_client(agent_id)

        await client.submit_agent_inquiry(
            sender_id=args.client_id,
            receiver_id=agent_id,
            session_id=session_id,
            content=text
        )

        print("Wait for the agent's reply...")
        result = ""
        while True:
            response = await client.receive_agent_feedback(session_id, receiver_id=agent_id)
            print(f"{response.sender_id} replys: {response.content}")
           
            for content_item in response.content:
                if content_item.WhichOneof('content') == 'text':
                    result += content_item.text
            
            if response.session_status == SessionStatus.STOP_RESPONSE:
                print("Agent has finished replying")
                break

        print(f"Final response content: {result}")

        await client.submit_agent_inquiry(
            sender_id=args.client_id,
            receiver_id=agent_id,
            session_id=session_id,
            content="End the session",
            session_status=SessionStatus.STOP_QUEST
        )
        
    except Exception as e:
        print(f"An uncaught exception occurred: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        
        if client is not None:
            try:
                print("Close the client connection of the gateway...")
                if hasattr(client, 'stop'):
                    await client.stop()
                else:
                    if hasattr(client, '_tool_clients'):
                        for toolbox_id in list(client._tool_clients.keys()):
                            await client.close_tool_client(toolbox_id)
                    if hasattr(client, '_agent_clients'):
                        for session_id, receiver_id in list(client._agent_clients.keys()):
                            pass
                print("The gateway client has been closed")
            except Exception as stop_e:
                print(f"The gateway client failed to close properly: {str(stop_e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--gateway_addr", default="localhost:50001")
    parser.add_argument("--client_id", default="test_gw_client")
    args = parser.parse_args()

    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        print("The program was interrupted by the user")
