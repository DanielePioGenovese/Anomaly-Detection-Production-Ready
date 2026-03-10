from langchain_openai import ChatOpenAI
from langchain_mcp_adapters.client import MultiServerMCPClient
from langgraph.prebuilt import create_react_agent
from config.config import inference_settings
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

client = MultiServerMCPClient(
    {
        'mcp_server': {
            'transport': 'streamable_http',
            'url': inference_settings.mcp_server_uri,
            'headers': {
                'X-Custom-Header': 'custom-value'
            }
        }
    }
)


async def build_agent():
    tools = await client.get_tools()
    logger.info(f"MCP Tools loaded: {[t.name for t in tools]}")

    llm = ChatOpenAI(
        model=inference_settings.chat_model,
        base_url=inference_settings.vllm_base_url,
        api_key='EMPTY',                   
        temperature=0.2,
        timeout=120,
        streaming=True,
    )

    agent = create_react_agent(
        llm,
        tools=tools,
        prompt=(
            'You are an anomaly investigation assistant. '
            'Use the available tools to retrieve context and diagnose the anomaly.'
        )
    )

    return agent