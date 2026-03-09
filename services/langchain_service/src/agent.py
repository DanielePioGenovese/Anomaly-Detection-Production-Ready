from langchain_openai import ChatOpenAI
from langchain_mc_adapters import MultiServerMCPClient
from langchain.agents import create_agent
from config import inference_settings

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("agent")

client = (
    {
        'mcp_interaction' : {
            'transport' : 'http',
            'url' : inference_settings.mcp_url,
            'headers' : {
                'X-Custom-Header' : 'custom_value'
            }
        }
    }
)


async def build_agent():
    tools = await client.get_tools()
    logger.info('MCP Tools', [t.name for t in tools])

    llm = ChatOpenAI(
        model=inference_settings.chat_model,
        temperature=0.2,
        timeout=120,
        streaming=True
    )

    agent = create_agent(
        llm,
        tools=tools,
        sysmtem_prompt=(
            'Example prompt'
        )
    )

    return agent