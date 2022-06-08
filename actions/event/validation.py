from typing import List, Tuple, Any
from data_base import main_db
from datetime import datetime
import os
import unidecode


class Validation:
    def __init__(self) -> None:
        self.db_instance = main_db.DBInstance(public_key="kKS0DfTKpE8TqUZs")

    def execute(self):
        # TODO: While Pagination
        migrated_schemas: List[Tuple[Any]] = self.get_migrated_schemas()
        self.validate_events(migrated_schemas=migrated_schemas)

    def validate_events(self, migrated_schemas):
        for migrated_schema in migrated_schemas:
            schema_properties = self.get_schema_properties(
                migrated_event_id=migrated_schema[0]
            )
            self.validate_event(
                event_name=migrated_schema[1], schema_properties=schema_properties
            )

    def validate_event(self, event_name, schema_properties):
        for event_property in self.join_user_event_properties(event_name=event_name):
            properties = list(
                filter(
                    lambda schema_property: self.clean_text(text=schema_property[1]) == self.clean_text(
                        text=event_property[2]),
                    schema_properties,
                )
            )
            if properties:
                if properties[0][2] == "text":
                    if isinstance(event_property[3], str):
                        self.update_valid_user_event(event_id=event_property[1])
                    else:
                        self.update_invalid_user_event(event_id=event_property[1])
                        break
                elif properties[0][2] == "numeric":
                    if isinstance(event_property[3], int):
                        self.update_valid_user_event(event_id=event_property[1])
                    else:
                        self.update_invalid_user_event(event_id=event_property[1])
                        break
                elif properties[0][2] == "date":
                    if isinstance(event_property[3], datetime):
                        self.update_valid_user_event(event_id=event_property[1])
                    else:
                        self.update_invalid_user_event(event_id=event_property[1])
                        break
            else:
                self.update_invalid_user_event(event_id=event_property[1])
                break

    def get_migrated_schemas(self) -> List[Tuple[Any]]:
        try:
            event_schemas = self.db_instance.handler(
                query="SELECT * FROM event_schema WHERE db_status = 'pending_create' AND name='SGC_SPEC';"
            )
        except Exception as e:
            raise e
        else:
            return event_schemas

    def get_generic_properties(self, name_event: str) -> List[Tuple[Any]]:
        try:
            generic_properties = self.db_instance.handler(
                query=f"""select name, count(*) from event_property where event_id in 
                    (select id from user_event where name='{name_event}') group by name"""
            )
        except Exception as e:
            raise e
        else:
            return generic_properties

    def get_schema_properties(self, migrated_event_id):
        try:
            event_properties = self.db_instance.handler(
                query=f"select * from property_event_schema where event_id={migrated_event_id};"
            )
        except Exception as e:
            raise e
        else:
            return event_properties

    @staticmethod
    def clean_text(text: str) -> str:
        text = unidecode.unidecode(
            text.replace("|", "")
            .replace(" ", "_")
            .replace("__", "_")
            .replace("___", "_")
        )
        return text.lower()

    def join_user_event_properties(self, event_name):
        try:
            user_events_properties = self.db_instance.handler(
                query=self.get_user_event_properties_query(event_name=event_name)
            )
        except Exception as e:
            raise e
        else:
            return user_events_properties

    @staticmethod
    def get_user_event_properties_query(event_name):
        query = f"""SELECT 
                          event_property.id, 
                          event_property.event_id, 
                          event_property.name, 
                          event_property.value, 
                          user_event.name, 
                          user_event.created_at, 
                          user_event.updated_at, 
                          user_event.valid, 
                          user_event.user_id 
                        FROM 
                          event_property 
                          INNER JOIN user_event ON event_property.event_id = user_event.id 
                        WHERE 
                          user_event.id in (
                            SELECT 
                              id 
                            FROM 
                              user_event 
                            WHERE 
                              name = '{event_name}' 
                              AND migrated = false 
                            limit 
                              100
                          ) 
                        ORDER BY 
                          user_event.id;
                    """
        return query

    def update_invalid_user_event(self, event_id):
        try:
            update_user_event = self.db_instance.handler(
                query=f"UPDATE user_event SET valid ='unvalid' WHERE id={event_id};"
            )
        except Exception as e:
            raise e
        else:
            return update_user_event

    def update_valid_user_event(self, event_id):
        try:
            update_user_event = self.db_instance.handler(
                query=f"UPDATE user_event SET valid ='validated' WHERE id={event_id};"
            )
        except Exception as e:
            raise e
        else:
            return update_user_event


if __name__ == "__main__":
    validation = Validation()
    print(validation.execute())