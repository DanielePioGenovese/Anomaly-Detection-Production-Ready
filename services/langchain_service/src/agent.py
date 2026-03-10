from langchain_openai import ChatOpenAI
from langchain_mcp_adapters.client import MultiServerMCPClient
from langchain.agents import create_agent
from config import inference_settings
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


client = MultiServerMCPClient(
    {
        'mcp_client': {
            'transport': 'http',
            'url': inference_settings.mcp_server_uri,
            'headers': { 
                'X-Custom-Header':'custom-value'
            }
        }
    }
)

async def build_agent():
    tools = await client.get_tools()
    logging.info(f"MCP Tools: {[t.name for t in tools]}")

    llm = ChatOpenAI(
        model=inference_settings.chat_model,
        temperature=0.2,
        timeout=120,
        streaming=True
    )

    agent = create_agent(
        llm,
        tools=tools,
        system_prompt=(
            'Example Prompt'
        )
    )

    return agent