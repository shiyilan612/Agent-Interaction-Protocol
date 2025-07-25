import requests
import asyncio
import argparse
from functools import partial
from atlink_aip.grpc_service.type import Mode
from atlink_aip.module import Agent


async def process_message(agent, args):
    async def call_LLM(url: str, api_key: str, model: str, text: str) -> str:
        headers = {
            "Authorization": api_key,
            "Content-Type": "application/json"
        }
        system_prompt = "You are an agent that communicates using the AIP protocol."
        user_prompt = f"{text}"
        params = {
            "model": model,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "stream": False
        }
        try:
            response = requests.post(url, json=params, headers=headers)
            response.raise_for_status()
            result = response.json()
            output = result['choices'][0]['message']['content']
            return output

        except Exception as e:
            return f"{str(e)}"

    try:
        while True:
            request = await agent.receive_inquiry()
            session_id = request.session_id
            handlers = [
                partial(
                    call_LLM,
                    url=args.llm_url,
                    api_key=args.api_key,
                    model=args.model,
                    text=request.content[0]._text
                )
            ]
            results = await agent.invoke_session_handlers(session_id, handlers)
            for text in results:
                await  agent.submit_feedback(
                    session_id=request.session_id,
                    receiver_id=request.sender_id,
                    request_session_status=request.session_status,
                    content=text,
                    content_mode=[Mode.TEXT]
                )
    except asyncio.CancelledError:
        await agent.stop()


async def main(args):
    agent = Agent(
        agent_id=args.agent_id,
        host_address=args.host_address,
        name=args.agent_name,
        description=f"LLM Mode:{args.model}"
    )

    await agent.start()
    await agent.register_to_gateway(args.gateway_address)

    try:
        asyncio.create_task(process_message(agent, args))
        await asyncio.sleep(float('inf'))
    except asyncio.CancelledError:
        await agent.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--llm_url", required=True, default="url address of LLM server")
    parser.add_argument("--api_key", required=True, default="api key of LLM server")
    parser.add_argument("--model", required=True, default="model name of LLM server")

    parser.add_argument("--agent_id", default="example_agent")
    parser.add_argument("--agent_name", default="LLM Agent")
    parser.add_argument("--gateway_address", default="localhost:50000")
    parser.add_argument("--host_address", default="localhost:52000")
    args = parser.parse_args()

    print(args.llm_url)
    print(args.api_key)
    print(args.model)

    asyncio.run(main(args))
