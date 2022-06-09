from typing import List, Tuple, Any
from data_base import validation_conn
from datetime import datetime
import os
import unidecode


class Validation:
    def __init__(self) -> None:
        self.db_conn = validation_conn.DBInstance(public_key="kKS0DfTKpE8TqUZs").make_conn()

    def execute(self):
        migrated_schemas: List[Tuple[Any]] = self.__get_migrated_schemas()
        self.validate_events(migrated_schemas=migrated_schemas)

    def __get_migrated_schemas(self) -> List[Tuple[Any]]:
        with self.db_conn.cursor() as curs:
            curs.execute("SELECT * FROM event_schema WHERE db_status = 'pending_create' AND name='SGC_SPEC';")
            return [line for line in curs.fetchall()]

    def validate_events(self, migrated_schemas):
        for schema in migrated_schemas:
            self.__validate_event(schema=schema)

        self.db_conn.close()

    def __validate_event(self, schema):
        for event in self.__get_events(event_name=schema[1]):
            for event_property in self.__get_event_properties(event_id=event[0]):
                schema_property = self.__get_schema_property(property_name=event_property[1])
                if schema_property[2] == "text":
                    try:
                        cast = str(event_property[2])
                    except ValueError:
                        self.update_invalid_user_event(event_id=event_property[1])
                        break
                    else:
                        if isinstance(cast, str):
                            self.update_valid_user_event(event_id=event_property[1])

                elif schema_property[2] == "numeric":
                    try:
                        cast = int(event_property[2])
                    except ValueError:
                        self.update_invalid_user_event(event_id=event_property[1])
                        break
                    else:
                        if isinstance(cast, int):
                            self.update_valid_user_event(event_id=event_property[1])

                elif schema_property[2] == "date":
                    try:
                        cast = datetime.strptime(event_property[2], '%d/%m/%y')
                    except ValueError:
                        self.update_invalid_user_event(event_id=event_property[1])
                        break
                    else:
                        if isinstance(cast, datetime):
                            self.update_valid_user_event(event_id=event_property[1])

    def __get_events(self, event_name):
        with self.db_conn.cursor() as curs:
            curs.execute(f"""
                SELECT 
                    * 
                FROM 
                    user_event 
                WHERE name = '{event_name}' AND migrated = false 
                LIMIT 100;
            """)
            return [line for line in curs.fetchall()]

    def __get_event_properties(self, event_id: int) -> List[Tuple[Any]]:
        with self.db_conn.cursor() as curs:
            curs.execute(f"""
                SELECT 
                    * 
                FROM 
                    event_property 
                WHERE event_id = {event_id};
            """)
            return [line for line in curs.fetchall()]

    def __get_schema_property(self, property_name: str) -> Tuple[Any]:
        with self.db_conn.cursor() as curs:
            curs.execute(f"""
                SELECT 
                    * 
                FROM 
                    property_event_schema
                WHERE name = '{property_name}';
            """)
            return curs.fetchone()

    def update_invalid_user_event(self, event_id) -> Tuple[Any]:
        with self.db_conn.cursor() as curs:
            curs.execute(f"UPDATE user_event SET valid ='unvalid' WHERE id={event_id};")
            return curs.fetchone()

    def update_valid_user_event(self, event_id):
        with self.db_conn.cursor() as curs:
            curs.execute(f"UPDATE user_event SET valid ='validated' WHERE id={event_id};")
            return curs.fetchone()

    @staticmethod
    def clean_text(text: str) -> str:
        text = unidecode.unidecode(
            text.replace("|", "")
            .replace(" ", "_")
            .replace("__", "_")
            .replace("___", "_")
        )
        return text.lower()


if __name__ == "__main__":
    validation = Validation()
    print(validation.execute())
