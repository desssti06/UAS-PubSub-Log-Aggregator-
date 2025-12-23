from pydantic import BaseModel
from typing import Optional, Dict

class Event(BaseModel):
    topic: str
    event_id: str
    payload: Dict
    timestamp: Optional[str] = None
    source: Optional[str] = None
