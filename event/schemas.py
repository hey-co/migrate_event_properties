from enum import IntEnum, Enum
from datetime import datetime, date
from typing import Optional, Union

from pydantic import BaseModel


class TypeDbStatus(str, Enum):
    MIGRATED = "migrated"
    PENDING_UPDATE = "pending_update"
    ERROR = "error"
    PENDING_CREATE = "pending_create"
    STATIC = "static"


class EventSchema(BaseModel):
    id: Optional[int]
    name: str
    help_name: Optional[str]
    description: Optional[str]
    db_status: TypeDbStatus
    updated_at: Union[datetime, date, None]
    created_at: Union[datetime, date, None]
    is_active: bool
    is_migrated: bool

    class Config:
        orm_mode = True