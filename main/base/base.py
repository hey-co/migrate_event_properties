from typing import List, Tuple, Any
from data_base import main_db

import os
import unidecode
import pandas as pd


class Base:
    def __init__(self) -> None:
        self.db_instance = main_db.DBInstance(public_key=os.environ["ELCOLOMBIANO"])

    def handler(self):
        migrated_schemas: List[Tuple[Any]] = self.get_migrated_schemas()

        for migrated_schema in migrated_schemas:
            generic_properties = self.get_generic_properties(migrated_schema[1])

            migrated_schema_properties = self.get_migrated_schema_properties(
                migrated_event_id=migrated_schema[0]
            )

            cleaned_names: list = list(
                map(
                    self.clean_name_properties,
                    [i[1] for i in migrated_schema_properties],
                )
            )

            event_names_1 = [
                event_name
                for event_name in [
                    n
                    for n in cleaned_names
                    if n not in [gp[0] for gp in generic_properties]
                ]
            ]

            event_names_2 = [
                event_name
                for event_name in [gp[0] for gp in generic_properties]
                if event_name in cleaned_names
            ]

            event_names = event_names_1 + event_names_2

            for event_name in event_names:
                self.get_user_events_properties(event_name=event_name)

    def get_user_events_properties_query(self, event_name):
        query = f"""SELECT event_property.id, event_property.event_id, event_property.name, 
            event_property.value, user_event.name, user_event.created_at,
            user_event.updated_at, user_event.valid, user_event.user_id
            FROM event_property
            INNER JOIN user_event ON event_property.event_id=user_event.id
            WHERE user_event.id in 
                (SELECT id FROM user_event WHERE name = '{event_name}' AND migrated=false limit 5000);"""
        return query

    def get_user_events_properties(self, event_name):
        try:
            user_events_properties = self.db_instance.handler(
                query=self.get_user_events_properties_query(event_name=event_name)
            )
        except Exception as e:
            raise e
        else:
            return self.get_df_events_properties(data=user_events_properties)

    def get_df_events_properties(self, data):
        columns = [
            "property_id",
            "event_id",
            "property_name",
            "property_value",
            "event_name",
            "event_created_at",
            "event_updated_at",
            "event_valid",
            "event_user_id",
        ]
        df = pd.DataFrame(data, columns=columns)

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

    def get_migrated_schema_properties(self, migrated_event_id):
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

    def clean_name_properties(self, name: str) -> str:
        name = unidecode.unidecode(
            name.replace("|", "").replace(" ", "_").replace("__", "_")
        )
        return name.upper()


if __name__ == "__main__":
    base = Base()
    base.handler()
