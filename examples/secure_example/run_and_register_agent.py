

import requests
import asyncio
import argparse
import traceback
from functools import partial
from ..grpc_service.type import Mode
from ..module import Agent

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
        return f"LLM调用失败: {str(e)}"

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
        print(f"消息处理出错: {str(e)}")
        traceback.print_exc()
    finally:
        #确保停止代理服务
        print("消息处理循环结束，准备关闭代理")
        await agent.stop()

async def main(args):
    agent = None
    try:
        print(f"启动代理服务: {args.agent_id}")
        agent = Agent(
            agent_id=args.agent_id,
            host_address=args.host_address,
            name=args.agent_name,
            description=f"LLM Mode:{args.model}",
        )
        
        #启用安全连接
        if args.with_auth:
            print("启用安全连接...")
            await agent.enable_security(
                private_key_path=args.private_key,
                public_key_path=args.public_key,
            )
        
        #启动代理并注册到网关
        print("启动服务器...")
        await agent.start()
        print(f"注册到网关: {args.gateway_address}")
        await agent.register_to_gateway(args.gateway_address)
        print(f"代理已成功注册，监听地址: {args.host_address}")
        
        #启动消息处理任务
        print("启动消息处理循环...")
        message_task = asyncio.create_task(process_message(agent, args))
        
        #等待服务运行
        print("代理服务运行中...按Ctrl+C退出")
        await asyncio.gather(message_task)
        
    except asyncio.CancelledError:
        print("代理服务被取消")
    except Exception as e:
        print(f"代理服务发生异常: {str(e)}")
        traceback.print_exc()
    finally:
        #确保关闭代理
        if agent is not None:
            try:
                print("关闭代理服务...")
                await agent.stop()
                print("代理服务已关闭")
            except Exception as stop_e:
                print(f"关闭代理时出错: {str(stop_e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--llm_url", required=True, default="url address of LLM server")
    parser.add_argument("--api_key", required=True, default="api key of LLM server")
    parser.add_argument("--model", required=True, default="model name of LLM server")
    parser.add_argument("--agent_id", default="example_agent")
    parser.add_argument("--agent_name", default="LLM Agent")
    parser.add_argument("--gateway_address", default="localhost:50000")
    parser.add_argument("--host_address", default="localhost:52000")
    parser.add_argument("--with_auth", action="store_true", help="启用安全连接")
    parser.add_argument("--private_key", default="server.key", help="服务端私钥路径")
    parser.add_argument("--public_key", default="ca.key", help="根公钥路径")
    args = parser.parse_args()

    print(f"LLM URL: {args.llm_url}")
    print(f"使用模型: {args.model}")
    
    try:
        asyncio.run(main(args))
    except KeyboardInterrupt:
        print("代理服务被用户中断")
