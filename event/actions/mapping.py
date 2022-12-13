from datetime import datetime
import os

from dotenv import load_dotenv
import unidecode
import pydantic

import multitenancy
from event import models, schemas
load_dotenv()


class Mapping:
    def __init__(self, event):
        self.db = self.get_session_db(private_key=event.get("private_key"))

    @staticmethod
    def get_session_db(private_key: str = None):
        dict_db_tenant = {
            "db_name": os.environ.get("MULTI_TENANCY_DB_NAME"),
            "db_user": os.environ.get("MULTI_TENANCY_DB_USER"),
            "db_password": os.environ.get("MULTI_TENANCY_DB_PASSWORD"),
            "db_port": os.environ.get("MULTI_TENANCY_DB_PORT"),
            "db_host": os.environ.get("MULTI_TENANCY_DB_HOST"),
        }
        return multitenancy.get_session_by_secret_key(private_key, dict_db_tenant)

    def get_distinct_user_event_names(self):
        try:
            distinct_names = self.db.query(models.Event.name).distinct()
        except Exception as e:
            raise e
        else:
            return distinct_names

    def write_event_schema(self, event_schema_name):
        try:
            event_schema = pydantic.parse_obj_as(
                schemas.EventSchema,
                {
                    "name": self.clean_text(text=event_schema_name),
                    "updated_at": datetime.now(),
                    "created_at": datetime.now(),
                    "help_name": event_schema_name,
                    "is_active": True,
                    "db_status": schemas.SqlStructureDbStatus.CREATE_PENDING,
                    "migrate_status": schemas.DataDbMigrateStatus.DB_PENDING,
                    "is_migrated": False
                }
            )
            new_event_schema = models.EventSchema(**event_schema.dict())
            self.db.add(new_event_schema)
        except Exception as e:
            raise e
        else:
            self.db.commit()

    @staticmethod
    def clean_text(text: str) -> str:
        text = unidecode.unidecode(
            text.replace("|", "")
            .replace(" ", "_")
            .replace("__", "_")
            .replace("___", "_")
        )
        return text.lower()

    def handle_valid_event_schema(self, event_schema_name):
        compare_properties = self.compare_properties(
            event_schema_name=event_schema_name
        )
        if compare_properties:
            self.update_event_schema_properties(
                data={
                    "event_schema_id": self.get_event_schema_id(
                        event_schema_name=event_schema_name
                    ),
                    "event_properties": compare_properties,
                }
            )

    def handle_invalid_event_schema(self, event_schema_name):
        self.write_event_schema(event_schema_name=event_schema_name)
        self.update_event_schema_properties(
            data={
                "event_schema_id": self.get_event_schema_id(
                    event_schema_name=event_schema_name
                ),
                "event_properties": self.get_event_properties_by_event_schema_name(
                    event_schema_name=event_schema_name
                ),
            }
        )

    def handler(self):
        for distinct_name in self.get_distinct_user_event_names():
            if self.validate_event_schema(event_schema_name=distinct_name[0]):
                self.handle_valid_event_schema(event_schema_name=distinct_name[0])
            else:
                self.handle_invalid_event_schema(event_schema_name=distinct_name[0])

    def get_event_schema_id(self, event_schema_name):
        try:
            return self.db.query(models.EventSchema).filter_by(name=self.clean_text(text=event_schema_name)).first().id
        except Exception as e:
            raise e

    def update_event_schema_properties(self, data):
        for event_property in data.get("event_properties"):
            self.write_schema_property(
                event_property=event_property,
                event_schema_id=data.get("event_schema_id"),
            )

    def write_schema_property(self, event_property, event_schema_id):
        try:
            schema_property = pydantic.parse_obj_as(
                schemas.EventSchemaProperty,
                {
                    "name": self.clean_text(text=event_property[0]),
                    "type": schemas.DataTypeColumn.TEXT,
                    "db_status": schemas.SqlStructureDbStatus.CREATE_PENDING,
                    "migrate_status": schemas.DataDbMigrateStatus.DB_PENDING,
                    "help_name": event_property[0],
                    "updated_at": datetime.now(),
                    "created_at": datetime.now(),
                    "is_active": True,
                    "event_id": event_schema_id
                }
            )
            new_schema_property = models.EventSchemaProperty(**schema_property.dict())
            self.db.add(new_schema_property)
        except Exception as e:
            raise e
        else:
            self.db.commit()

    def get_schema_properties_names(self, event_schema_name):
        return [
            sp[0]
            for sp in self.get_schema_properties_by_event_schema_name(
                event_schema_name=event_schema_name
            )
        ]

    def get_result_properties(self, schema_properties_names, schema_properties_help_names, event_schema_name):
        result_properties = []
        for ep in self.get_event_properties_by_event_schema_name(event_schema_name=event_schema_name):
            if self.clean_text(text=ep[0]) not in schema_properties_names or ep[0] not in schema_properties_help_names:
                result_properties.append(ep)
        return result_properties

    def compare_properties(self, event_schema_name):
        return self.get_result_properties(
            schema_properties_names=self.get_schema_properties_names(
                event_schema_name=event_schema_name
            ),
            schema_properties_help_names=self.get_schema_properties_help_names(
                event_schema_name=event_schema_name
            ),
            event_schema_name=event_schema_name,
        )

    def get_schema_properties_help_names(self, event_schema_name):
        return [
            sp[0]
            for sp in self.get_schema_properties_help_names_query(
                event_schema_name=event_schema_name
            )
        ]

    def get_schema_properties_help_names_query(self, event_schema_name):
        try:
            return self.db.query(models.EventSchemaProperty.help_name).filter(
                models.EventSchemaProperty.event_id.in_(
                    self.db.query(models.EventSchema.id).filter_by(name=self.clean_text(text=event_schema_name))
                )
            ).distinct()
        except Exception as e:
            raise e

    def get_schema_properties_by_event_schema_name(self, event_schema_name):
        try:
            return self.db.query(models.EventSchemaProperty.name).filter(
                models.EventSchemaProperty.event_id.in_(
                    self.db.query(models.EventSchema.id).filter_by(name=self.clean_text(text=event_schema_name))
                )
            ).distinct()
        except Exception as e:
            raise e

    def get_event_properties_by_event_schema_name(self, event_schema_name):
        try:
            return self.db.query(models.PropertyEvent.name).filter(
                models.PropertyEvent.event_id.in_(
                    self.db.query(models.Event.id).filter_by(name=event_schema_name)
                )
            ).distinct()
        except Exception as e:
            raise e

    def validate_event_schema(self, event_schema_name: str) -> bool:
        event_schema_name_validation = self.validate_event_schema_by_name(
            event_schema_name=event_schema_name
        )
        event_schema_help_name_validation = self.validate_event_schema_by_help_name(
            event_schema_name=event_schema_name
        )
        return event_schema_name_validation or event_schema_help_name_validation

    def validate_event_schema_by_help_name(self, event_schema_name):
        try:
            validate_column = self.db.query(models.EventSchema).filter_by(help_name=event_schema_name).first()
        except Exception as e:
            raise e
        else:
            return validate_column

    def validate_event_schema_by_name(self, event_schema_name):
        try:
            validate_column = self.db.query(models.EventSchema).filter_by(
                name=self.clean_text(text=event_schema_name)
            ).first()
        except Exception as e:
            raise e
        else:
            return validate_column


def lambda_handler(event, context):
    return Mapping(event=event).handler()


if __name__ == '__main__':
    lambda_handler(
        event={
            "private_key": "nGPoVSbPAZtbawqf"
        },
        context={}
    )
