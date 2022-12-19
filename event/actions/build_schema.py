from sqlalchemy import Column, Integer, Boolean, String, DateTime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class EventSchema(Base):

    __tablename__ = "event_schema"

    id = Column(Integer, primary_key=True)
    name = Column(String(255), nullable=False)
    help_name = Column(String(254), nullable=True)
    description = Column(String(2000), nullable=False)
    db_status = Column(Integer, nullable=False)
    migrate_status = Column(Integer, nullable=False)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    is_active = Column(Boolean, default=True, nullable=False)
    is_migrated = Column(Boolean, default=True, nullable=False)

    def __repr__(self):
        return "<UserEvent(user_id='%s', value='%s' )>" % (self.user_id, self.value)

    def __init__(
            self,
            name,
            help_name,
            description,
            db_status,
            migrate_status,
            created_at,
            updated_at,
            is_active,
            is_migrated,
            keys=[],
            values=[]
    ):
        self.name = name
        self.help_name = help_name
        self.description = description
        self.db_status = db_status
        self.migrate_status = migrate_status
        self.is_active = is_active
        self.is_migrated = is_migrated
        self.created_at = created_at
        self.updated_at = updated_at

        for (key, value) in zip(keys, values):
            self.__dict__[key] = value


class BuildSchema:
    def __init__(self, event):
        self.columns = event.get("columns")
        self.schema = EventSchema()

    def add_columns(self):
        for column in self.columns:
            pass

    def handler(self):
        return self.add_columns()


def lambda_handler():
    return BuildSchema.handler()

