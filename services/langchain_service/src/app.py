import json
import asyncio  
from fastapi import FastAPI
from fastapi.responses import StreamingResponse 
from langchain_core.messages import HumanMessage  
from langchain_service.src import build_agent, ChatRequest


app = FastAPI()

_agent = None
_agent_lock = asyncio.Lock()


async def get_agent():
    global _agent
    if _agent is None:
        async with _agent_lock:
            if _agent is None:
                _agent = await build_agent()
    return _agent

def sse_pack(event: str, data) -> str:
    return f'event: {event}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n'

@app.post('/chat/stream')
async def chat_stream(req: ChatRequest)