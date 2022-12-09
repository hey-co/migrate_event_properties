from datetime import datetime

from sqlalchemy import Column, Integer, String
from sqlalchemy import DateTime, Boolean
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class EventSchema(Base):
    __tablename__ = "event_schema"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    help_name = Column(String(254), nullable=True)
    description = Column(String(2000), nullable=True)
    status = Column(String(100), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.now())
    updated_at = Column(DateTime, nullable=False, default=datetime.now())
    is_active = Column(Boolean, default=True, nullable=False)
    is_migrated = Column(Boolean, default=False, nullable=False)

    def __repr__(self):
        return "<Event schema(id='%s', name='%s' )>" % (self.id, self.name)


class EventSchemaProperty(Base):
    __tablename__ = "property_event_schema"

    id = Column(Integer, primary_key=True)
    name = Column(String(5000), nullable=False)
    event_id = Column(Integer, nullable=False)
    help_name = Column(String(254), nullable=True)
    type = Column(String(100), nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.now())
    updated_at = Column(DateTime, nullable=False, default=datetime.now())
    is_active = Column(Boolean, default=True, nullable=False)

    def __repr__(self):
        return "<Property Event schema(id='%s', name='%s' )>" % (self.id, self.name)


class Event(Base):
    __tablename__ = "user_event"

    id = Column(Integer, primary_key=True)
    name = Column(String(128), nullable=True)
    value = Column(String(2000), nullable=True)
    email = Column(String(2000), nullable=True)
    valid = Column(String(100), nullable=True)
    migrated = Column(Boolean, default=False, nullable=True)
    user_id = Column(Integer, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.now())
    updated_at = Column(DateTime, nullable=False, default=datetime.now())

    def __repr__(self):
        return "<User Event(id='%s', name='%s' )>" % (self.id, self.name)


class PropertyEvent(Base):
    __tablename__ = "event_property"

    id = Column(Integer, primary_key=True)
    name = Column(String(128), nullable=True)
    value = Column(String(2000), nullable=True)
    event_id = Column(Integer, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.now())
    updated_at = Column(DateTime, nullable=False, default=datetime.now())

    def __repr__(self):
        return "<Event property(id='%s', name='%s' )>" % (self.id, self.name)
