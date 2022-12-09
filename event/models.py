from datetime import datetime

from sqlalchemy import Column, Integer, String
from sqlalchemy import DateTime, Text, Boolean
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class EventSchema(Base):
    __tablename__ = "event_schema"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    help_name = Column(String(254), nullable=True)
    description = Column(String(2000), nullable=True)
    status = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.now())
    updated_at = Column(DateTime, nullable=False, default=datetime.now())
    is_active = Column(Boolean, default=True, nullable=False)
    is_migrated = Column(Boolean, default=False, nullable=False)

    def __repr__(self):
        return "<Event schema(id='%s', name='%s' )>" % (self.id, self.name)
