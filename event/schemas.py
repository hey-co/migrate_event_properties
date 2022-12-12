from enum import Enum, IntEnum
from datetime import datetime, date
from typing import Optional, Union

from pydantic import BaseModel


class SqlStructureDbStatus(IntEnum):
    EDIT_IN_PROGRESS = 1
    CREATE_PENDING = 2
    CREATE_IN_PROGRESS = 3
    CREATE_FAILED = 4
    CREATE_COMPLETED = 5
    ALTER_TABLE_IN_PROGRESS = 6


class DataDbMigrateStatus(IntEnum):
    DB_PENDING = 1
    MIGRATE_PENDING = 2
    MIGRATE_IN_PROGRESS = 3
    MIGRATE_FAILED = 4
    MIGRATE_COMPLETED = 5


class DataTypeColumn(str, Enum):
    TEXT = 'varchar'
    FLOAT = "numeric"
    DATE = "date"
    INTEGER = "integer"


class EventSchema(BaseModel):
    id: Optional[int]
    name: str
    help_name: Optional[str]
    description: Optional[str]
    db_status: SqlStructureDbStatus
    migrate_status: DataDbMigrateStatus
    updated_at: Union[datetime, date, None]
    created_at: Union[datetime, date, None]
    is_active: bool
    is_migrated: bool

    class Config:
        orm_mode = True


class EventSchemaProperty(BaseModel):
    id: Optional[int]
    name: str
    event_id: int
    help_name: Optional[str]
    type: DataTypeColumn
    db_status: SqlStructureDbStatus
    migrate_status: DataDbMigrateStatus
    updated_at: Union[datetime, date, None]
    created_at: Union[datetime, date, None]
    is_active: bool

    class Config:
        orm_mode = True


class UserEvent(BaseModel):
    id: Optional[int]
    name: Optional[str]
    value: Optional[str]
    email: Optional[str]
    valid: Optional[str]
    migrated: Optional[bool]
    user_id: int
    updated_at: Union[datetime, date, None]
    created_at: Union[datetime, date, None]

    class Config:
        orm_mode = True


class PropertyEvent(BaseModel):
    id: Optional[int]
    name: Optional[str]
    value: Optional[str]
    event_id: int
    updated_at: Union[datetime, date, None]
    created_at: Union[datetime, date, None]

    class Config:
        orm_mode = True
