import json
import argparse
import asyncio
from atlink_aip.module import Agent
from atlink_aip.grpc_service.type import AgentInfo, ToolBoxInfo, SessionStatus

async def main(args):
    agent = Agent(
        agent_id=args.agent_id,
        host_address=args.host_address,
        name=args.agent_name,
        description="",
    )

    await agent.start()
    await agent.register_to_gateway(args.gateway_address)

    # To obtain the node information in the gateway, it can list all accessible tools and agents.
    await agent.update_peers()
    gateway_peers = await agent.get_nodes()
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
            if peer.agent_id == args.agent_id:
                continue

            print(f"id: {peer.agent_id}")
            print(f"name: {peer.name}")
            print(f"domain: {peer.domain}")
            print(f"description: {peer.description}")
        print("")

    # the test invokes the tool through the gateway.
    toolbox_id = "example_tool"
    await agent.create_tool_client(receiver_id=toolbox_id)
    response = await agent.call_tool(
        toolbox_id="example_tool",
        tool_name="calculate_sum",
        arguments=json.dumps({"a": 111, "b": 333})
    )
    await agent.close_tool_client(toolbox_id=toolbox_id)
    print(f"Results from example_tool: {response}")
    print("")

    # the test invokes the other agent through the gateway.
    agent_id = "example_agent"
    session_id = await agent.create_agent_client(agent_id)
    await agent.submit_inquiry(
        session_id=session_id,
        receiver_id=agent_id,
        content="Who are you?"
    )
    await agent.submit_inquiry(
        session_id=session_id,
        receiver_id=agent_id,
        content="GoodBye!",
        session_status=SessionStatus.STOP_QUEST
    )

    while True:
        response = await agent.receive_feedback(session_id, receiver_id=agent_id)

        result = ""
        for content_item in response.content:
            if content_item._text:
                result += content_item._text
        print(f"Response received from <{response.sender_id}>: {result}")

        if response.session_status == SessionStatus.STOP_RESPONSE:
            break

    # stop agent
    await agent.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--agent_id", default="agent client")
    parser.add_argument("--agent_name", default="Dummy Agent")
    parser.add_argument("--gateway_address", default="localhost:50000")
    parser.add_argument("--host_address", default="localhost:53000")
    args = parser.parse_args()


    asyncio.run(main(args))
