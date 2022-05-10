from typing import List, Tuple, Any, Dict
from data_base import main_db
from psycopg2 import extras

import psycopg2
import os
import logging

logger = logging.getLogger()


class Cast:
    def __init__(self) -> None:
        self.db_instance = main_db.DBInstance(public_key=os.environ["ELCOLOMBIANO"])

    def testing_function(self):
        migrated_schemas: List[Tuple[Any]] = self.get_migrated_schemas()

        for migrated_schema in migrated_schemas:

            generic_events = self.get_generic_events_name(schema_name=migrated_schema[1])

            for generic_event in generic_events:
                generic_properties = self.get_generic_properties(event_id=generic_event[0])


            event_schema_properties = self.get_event_schema_properties(event_id=migrated_schema[0])

            for event_schema_property in event_schema_properties:
                event_properties = self.testing_event_properties(property_id=event_schema_property[0])

                #Posible for event_properties

                self.execute_cast(value=event_properties[0][0], to_cast=event_schema_property[2])


    def get_generic_properties(self, event_id):
        try:
            generic_properties = self.db_instance.handler(
                query=f"SELECT * FROM event_property WHERE event_id=event_id limit 100;"
            )
        except Exception:
            raise Exception
            return []
        else:
            return generic_properties

    def get_generic_events_name(self, schema_name):
        try:
            generic_schemas = self.db_instance.handler(
                query=f"SELECT * FROM user_event WHERE name = '{schema_name}' limit 100;"
            )
        except Exception:
            raise Exception
            return []
        else:
            return generic_schemas

    def execute_cast(self, value: Any, to_cast: Any):
        try:
            cast = self.db_instance.handler(
                query=f"SELECT {value}::{to_cast};"
            )
        except Exception:
            raise Exception
        else:
            return cast

    def testing_event_properties(self, property_id):
        event_properties = self.db_instance.handler(
            query=f"SELECT id, value FROM event_property WHERE id={property_id} limit 100;"
        )
        return event_properties

    def get_event_schema_properties(self, event_id: int) -> List[Tuple[Any]]:
        try:
            event_schemas_properties = self.db_instance.handler(
                query="SELECT id, name, type FROM property_event_schema WHERE event_id = event_id limit 100;"
            )
        except Exception:
            raise Exception
            return []
        else:
            return event_schemas_properties

    def get_migrated_schemas(self) -> List[Tuple[Any]]:
        try:
            event_schemas = self.db_instance.handler(
                query="SELECT * FROM event_schema WHERE db_status = 'pending_create';"
            )
        except Exception:
            raise Exception
            return []
        else:
            return event_schemas


if __name__ == "__main__":
    main = Main()
    main.execute()
