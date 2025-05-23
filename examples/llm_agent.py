# -*- coding: utf-8 -*-
"""
Created on Thu May 09 10:15:23 2025

@author: xmkang
"""
import os
import sys
import argparse
import asyncio
import json
import openai
from functools import partial
from typing import Dict, List, Tuple, Union
from atlink.grpc_service.type import AgentInfo, ToolInfo, AgentMessage
from atlink.grpc_service.type import AgentSkill, SessionStatus, Mode
from atlink.module import Agent


class LLMConfig:
    """Configuration for LLM-based agents."""

    def __init__(self,
                 api_url: str,
                 api_key: str = None,
                 model_name: str = None,
                 headers: Dict[str, str] = None,
                 timeout: int = 60,
                 temperature: float = 0.7,
                 max_tokens: int = 1024):
        """
        Initialize LLM configuration.

        Args:
            api_url: The API endpoint URL for the LLM
            api_key: API key for authentication
            model_name: Name of the model to use
            headers: Additional HTTP headers
            timeout: Request timeout in seconds
            temperature: Controls randomness (0.0-1.0)
            max_tokens: Maximum number of tokens to generate
        """
        self.api_url = api_url
        self.api_key = api_key
        self.model_name = model_name
        self.headers = headers or {}
        self.timeout = timeout
        self.temperature = temperature
        self.max_tokens = max_tokens


class LLMAgent(Agent):
    """
    An agent that integrates with Large Language Models for processing and responding to messages.
    """

    def __init__(self,
                 address: str,
                 llm_provider: str,
                 llm_config: LLMConfig,
                 system_prompt: str = None,
                 agent_id: str = None,
                 name: str = None,
                 domain: str = "default",
                 description: str = "",
                 version: str = "1.0.0",
                 input_mode: List[Mode] = [Mode.TEXT],
                 output_mode: List[Mode] = [Mode.TEXT],
                 skills: List[AgentSkill] = None):
        """
        Initialize an LLM-powered agent.

        Args:
            address: Address where this agent will be hosted
            llm_provider: Provider for LLM access ('local', 'openai', etc.)
            llm_config: Configuration for the LLM
            system_prompt: System prompt to guide the LLM's behavior
            agent_id: Unique identifier for this agent
            name: Human-readable name for this agent
            domain: Agent group/domain
            description: Detailed description of the agent's functionality
            version: Agent version
            input_mode: Expected input modality
            output_mode: Output modality provided by the agent
            skills: List of skills this agent possesses
        """
        super().__init__(
            address=address,
            agent_id=agent_id,
            name=name,
            domain=domain,
            description=description,
            version=version,
            input_mode=input_mode,
            output_mode=output_mode,
            skills=skills
        )

        self.llm_provider = llm_provider
        self.llm_config = llm_config
        self.system_prompt = system_prompt or self._default_system_prompt()

        # Node cache
        self._available_agents: Dict[str, AgentInfo] = {}  # agent_id -> agent_info
        self._available_tools: Dict[str, ToolInfo] = {}  # tool_id -> tool_info

        # Initialize LLM client
        self._llm_client = None
        self._initialize_llm_client()

    def _initialize_llm_client(self):
        """Initialize the LLM client based on the provider."""
        try:
            self._llm_client = openai.OpenAI(base_url=self.llm_config.api_url,
                                             api_key=self.llm_config.api_key)
        except ImportError:
            raise ImportError("Failed to initialize llm client")

    async def _fetch_available_nodes(self):
        """Fetch information about available nodes from the gateway."""
        nodes = await self.get_nodes()

        for node_id, node_info in nodes.items():
            if hasattr(node_info, 'tool_id'):  # It's a tool
                self._available_tools[node_id] = node_info
            elif hasattr(node_info, 'agent_id'):  # It's an agent
                self._available_agents[node_id] = node_info

    def _default_system_prompt(self):
        system_prompt = f"""You are an AI agent named {self.name} """
        if self.description:
            system_prompt += f"""with the following description: {self.description}"""
        if self.skills:
            system_prompt += f"""You have the following skills:\n"""
            for skill in self.skills:
                system_prompt += f"""- {skill.capability}\n"""
        return system_prompt

    def _prepare_llm_prompt(self,
                            message: AgentMessage,
                            session_id: str) -> Dict:
        """Prepare the prompt to send to the LLM."""
        # Extract message content
        message_content = ""
        for content_item in message.content:
            if content_item._text:
                message_content += content_item._text

        # available tools information
        tool_prompt = ""
        if self._available_tools:
            tool_prompt += "You can use these tools:\n"
            for tool_id, tool_info in self._available_tools.items():
                tool_prompt += f"- {tool_info.name} (ID: {tool_id}): {tool_info.description}\n"
                if tool_info.arguments:
                    tool_prompt += "  Arguments:\n"
                    for arg_name, arg_desc in tool_info.arguments.items():
                        tool_prompt += f"  - {arg_name}: {arg_desc}\n"

        # available agents information
        agent_prompt = ""
        if self._available_agents:
            agent_prompt += "You can collaborate with these agents:\n"
            for agent_id, agent_info in self._available_agents.items():
                if agent_id != self.agent_id:
                    agent_prompt += f"- {agent_info.name} (ID: {agent_id}): {agent_info.description}\n"
                if agent_info.skills:
                    agent_prompt += "  Skills:\n"
                    for skill in agent_info.skills:
                        agent_prompt += f"  - {skill.capability}\n"
        # Create the full prompt
        system_prompt = self.system_prompt
        system_prompt += f"""You are interacting with a user or another agent who sent you queries.
        You can respond directly.\n{tool_prompt}\n{agent_prompt}"""

        system_prompt += """Respond in one of these formats:
        1. Direct response:
        RESPONSE: Your direct response to the query

        2. To use a tool:
        TOOL_CALL: {"tool_id": "the tool_id", "tool_name": "the tool_name", "arguments": {"arg1": "value1", "arg2": "value2"}}

        3. To ask another agent:
        ASK_AGENT: {"agent_id": "the agent_id", "message": "Your message to the agent"}

        Choose the most appropriate response type based on the query."""

        llm_prompt = {
            "system": system_prompt,
            "user_message": message_content
        }

        return llm_prompt

    async def _call_llm(self, prompt: Dict) -> str:
        """Make the actual call to the LLM."""
        try:
            response = await asyncio.to_thread(
                self._llm_client.chat.completions.create,
                model=self.llm_config.model_name,
                messages=[{"role": "system", "content": prompt["system"]}] +
                         [{"role": "user", "content": prompt["user_message"]}],
                max_tokens=self.llm_config.max_tokens,
                temperature=self.llm_config.temperature
            )
            return response.choices[0].message.content
        except Exception as e:
            print(f"Error calling LLM: {str(e)}")
            return f"Error generating response: {str(e)}"

    async def process_server_message(self):
        while True:
            request = await self.receive_inquiry()
            session_id = request.session_id
            handlers = [
                partial(self._process_llm_request, message=request)
            ]
            results = await self.invoke_session_handlers(session_id, handlers)
            for text in results:
                await  self.submit_feedback(
                    session_id=request.session_id,
                    receiver_id=request.sender_id,
                    content=text,
                    content_mode=[Mode.TEXT],
                    request_session_status=request.session_status
                )

    async def _process_llm_request(self, message: AgentMessage) -> str: #AgentMessage:
        """Process incoming messages using the LLM."""
        session_id = message.session_id
        sender_id = message.sender_id

        # Ensure we have agent/tool information
        if not self._available_agents and self._available_tools:
            await self._fetch_available_nodes()

        # Prepare the prompt
        llm_prompt = self._prepare_llm_prompt(message, session_id)

        # Call the LLM
        llm_response = await self._call_llm(llm_prompt)
        # Parse the response for potential actions
        action, content = self._parse_llm_response(llm_response)

        if action == "call_tool":
            # Handle tool calling
            tool_response = await self._handle_tool_call(content, session_id)
            return tool_response
        elif action == "ask_agent":
            # Handle asking another agent
            agent_response = await self._handle_agent_call(content)
            return agent_response
        else:
            # Direct response
            pass
        return content

    def _parse_llm_response(self, response: str) -> Tuple[str, Union[str, Dict]]:
        """
        Parse LLM response to determine if it contains an action.

        Returns:
            Tuple of (action_type, content)
        """
        # Check for tool call
        if "TOOL_CALL:" in response:
            try:
                tool_part = response.split("TOOL_CALL:", 1)[1].strip()
                # Extract the JSON part
                tool_info = json.loads(tool_part.strip())
                return "call_tool", tool_info
            except (json.JSONDecodeError, IndexError):
                # If parsing fails, treat as direct response
                return "direct", response

        # Check for agent call
        elif "ASK_AGENT:" in response:
            try:
                agent_part = response.split("ASK_AGENT:", 1)[1].strip()
                # Extract the JSON part
                agent_info = json.loads(agent_part.strip())
                return "ask_agent", agent_info
            except (json.JSONDecodeError, IndexError):
                # If parsing fails, treat as direct response
                return "direct", response

        # Check for direct response marker
        elif "RESPONSE:" in response:
            direct_part = response.split("RESPONSE:", 1)[1].strip()
            return "direct", direct_part

        # Default to direct response if no markers found
        return "direct", response

    async def _handle_tool_call(self, tool_info: Dict, session_id: str) -> str:
        """
        Handle a tool call from LLM response.

        Args:
            tool_info: Dictionary with tool_id, tool_name, and arguments
            session_id: Current session ID to maintain conversation context

        Returns:
            Result from the tool as a string
        """

        tool_id = tool_info.get("tool_id")
        tool_name = tool_info.get("tool_name")
        arguments = tool_info.get("arguments", {})

        if tool_id in self._available_tools:
            try:
                # Call the tool
                response = await self.call_tool(
                    tool_id=tool_id,
                    tool_name=tool_name,
                    session_id=session_id,
                    arguments=arguments
                )

                # Extract response content
                result = ""
                for content_item in response.content:
                    if content_item._text:
                        result += content_item._text
                return result

            except Exception as e:
                return f"Error calling tool: {str(e)}"
        else:
            return f"Illegal tool_id"

    async def _handle_agent_call(self, agent_info: Dict) -> str:
        """
        Handle asking another agent.

        Args:
            agent_info: Dictionary with agent_id and message
            #session_id: Current session ID to maintain conversation context

        Returns:
            Response from the agent as a string
        """
        agent_id = agent_info.get("agent_id")
        message = agent_info.get("message", "")

        if agent_id in self._available_agents:
            try:
                session_id = await self.create_agent_client(receiver_id=agent_id)
                await self.submit_inquiry(
                    session_id=session_id,
                    receiver_id=agent_id,
                    content=message
                )
                await self.submit_inquiry(
                    session_id=session_id,
                    receiver_id=agent_id,
                    content="Finish Talk",
                    session_status = SessionStatus.STOP_QUEST
                )

                result = ""
                while True:
                    response = await self.receive_feedback(session_id=session_id, receiver_id=agent_id)
                    if response.session_status == SessionStatus.STOP_RESPONSE:
                        break
                    for content_item in response.content:
                        if content_item._text:
                            result += content_item._text
                return result

            except Exception as e:
                return f"Error communicating with agent: {str(e)}"
        else:
            return f"Illegal agent_id"

async def main(agent_id: str, agent_address: str, gateway_address: str):
    # Create and start Assistant Agent
    llmconfig = LLMConfig(
        api_url = "http://172.18.36.90:8106/v1",
        api_key = "Empty",
        model_name = "Qwen7B"
    )
    assistant = LLMAgent(
        llm_provider="vllm",
        llm_config=llmconfig,
        address=agent_address,
        agent_id=agent_id,
        name="Assistant Agent",
        description="A LLM_based agent that provides assistance"
    )
    await assistant.start()
    await assistant.register_to_gateway(gateway_address)
    server_task = asyncio.create_task(assistant.process_server_message())
    print("Assistant llm_agent registered")

    await assistant.update_peers()
    await assistant._fetch_available_nodes()
    await asyncio.sleep(9999999)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()


    parser.add_argument("--agent_id", type=str, required=True, help="Create an agent ID")
    parser.add_argument("--agent_address", type=str, default="localhost:50052")
    parser.add_argument("--gateway_address", default="localhost:50050")
    args = parser.parse_args()

    asyncio.get_event_loop().set_debug(True)
    asyncio.run(main(agent_id=args.agent_id, agent_address=args.agent_address, gateway_address=args.gateway_address))

