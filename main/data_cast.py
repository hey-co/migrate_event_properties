from typing import List, Tuple, Any
from data_base import main_db

import os


class Cast:
    def __init__(self) -> None:
        self.db_instance = main_db.DBInstance(public_key=os.environ["ELCOLOMBIANO"])

    def execute(self):
        migrated_schemas: List[Tuple[Any]] = self.get_migrated_schemas()

        for migrated_schema in migrated_schemas:

            generic_properties = self.get_generic_properties(migrated_schema[1])

            migrated_event_properties = self.get_migrated_event_properties(
                migrated_event_id=migrated_schema[0]
            )

            """
            self.execute_cast(
                value=event_properties[0][0], to_cast=event_schema_property[2]
            )
            """

    def execute_cast(self, value: Any, to_cast: Any):
        try:
            cast = self.db_instance.handler(query=f"SELECT {value}::{to_cast};")
        except Exception as e:
            raise e
        else:
            return cast

    def get_generic_properties(self, name_event):
        try:
            generic_properties = self.db_instance.handler(
                query=f"""select name, count(*) from event_property where event_id in 
                    (select id from user_event where name='{name_event}') group by name"""
            )
        except Exception as e:
            raise e
        else:
            return generic_properties

    def get_migrated_event_properties(self, migrated_event_id):
        try:
            event_properties = self.db_instance.handler(
                query=f"select * from property_event_schema where event_id={migrated_event_id};"
            )
        except Exception as e:
            raise e
        else:
            return event_properties

    def get_migrated_schemas(self) -> List[Tuple[Any]]:
        try:
            event_schemas = self.db_instance.handler(
                query="SELECT * FROM event_schema WHERE db_status = 'pending_create';"
            )
        except Exception as e:
            raise e
        else:
            return event_schemas


if __name__ == "__main__":
    main = Cast()
    main.execute()
