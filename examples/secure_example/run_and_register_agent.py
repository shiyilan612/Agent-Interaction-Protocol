import requests
import asyncio
import argparse
import traceback
from functools import partial
from atlink_aip.grpc_service.type import Mode
from atlink_aip.module import Agent

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
        return f"LLM invocation failed: {str(e)}"

async def process_message(agent, args):
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
                await agent.submit_feedback(
                    session_id=request.session_id,
                    receiver_id=request.sender_id,
                    request_session_status=request.session_status,
                    content=text,
                    content_mode=[Mode.TEXT]
                )
    except Exception as e:
        print(f"Error processing message: {str(e)}")
        traceback.print_exc()
    finally:
        print("Message processing loop ended, preparing to close agent")
        await agent.stop()

async def main(args):
    agent = None
    try:
        print(f"Starting agent service: {args.agent_id}")
        agent = Agent(
            agent_id=args.agent_id,
            host_address=args.host_address,
            name=args.agent_name,
            description=f"LLM Mode:{args.model}",
        )
        
        if args.with_auth:
            print("Enabling secure connection...")
            await agent.enable_security(
                private_key_path=args.private_key,
                public_key_path=args.public_key,
            )
        
        print("Starting server...")
        await agent.start()
        print(f"Registering to gateway: {args.gateway_address}")
        await agent.register_to_gateway(args.gateway_address)
        print(f"Agent registered successfully, listening on: {args.host_address}")
        
        print("Starting message processing loop...")
        message_task = asyncio.create_task(process_message(agent, args))
        
        print("Agent service running... Press Ctrl+C to exit")
        await asyncio.gather(message_task)
        
    except asyncio.CancelledError:
        print("Agent service cancelled")
    except Exception as e:
        print(f"Agent service encountered an exception: {str(e)}")
        traceback.print_exc()
    finally:
        if agent is not None:
            try:
                print("Closing agent service...")
                await agent.stop()
                print("Agent service closed")
            except Exception as stop_e:
                print(f"Error closing agent: {str(stop_e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--llm_url", required=True, default="url address of LLM server")
    parser.add_argument("--api_key", required=True, default="api key of LLM server")
    parser.add_argument("--model", required=True, default="model name of LLM server")
    parser.add_argument("--agent_id", default="example_agent")
    parser.add_argument("--agent_name", default="LLM Agent")
    parser.add_argument("--gateway_address", default="localhost:50000")
    parser.add_argument("--host_address", default="localhost:52000")
    parser.add_argument("--with_auth", action="store_true", help="Enable secure connection")
    parser.add_argument("--private_key", default="server.key", help="Server private key path")
    parser.add_argument("--public_key", default="ca.key", help="Root public key path")
    args = parser.parse_args()

    print(f"LLM URL: {args.llm_url}")
    print(f"Using model: {args.model}")
    
    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        print("Agent service interrupted by user")