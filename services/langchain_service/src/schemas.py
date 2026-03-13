from pydantic import BaseModel


class ChatRequest(BaseModel):
    message: str 
    machine_id: str = "unknown"
