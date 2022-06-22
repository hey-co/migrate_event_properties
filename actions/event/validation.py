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
            schema_properties = self.__get_schema_properties(
                schema_name=schema[1]
            )

            if self.__validate_schema(schema_properties=schema_properties, schema_name=schema[1]):
                self.__validate_event(schema=schema, schema_properties=schema_properties)

        self.db_conn.close()

    def __get_schema_properties(self, schema_name: str) -> List[Tuple[Any]]:
        with self.db_conn.cursor() as curs:
            curs.execute(f"""
                SELECT
                    property_event_schema.name,
                    property_event_schema.type
                FROM
                    property_event_schema
                    INNER JOIN event_schema ON property_event_schema.event_id = event_schema.id
                WHERE
                    event_schema.name like '{schema_name}'
                ORDER BY
                    property_event_schema.name;
                """)

            return [line for line in curs.fetchall()]

    def __validate_schema(self, schema_properties, schema_name):
        validation_dict = {}

        generic_properties_names = [
            self.clean_text(text=i[0])
            for i in self.__get_generic_properties(event_name=schema_name)
        ]

        schema_properties_names = [
            self.clean_text(text=sp[0])
            for sp in schema_properties
        ]

        for gpn in generic_properties_names:
            if gpn in schema_properties_names:
                validation_dict[gpn] = True
            else:
                return False
        if validation_dict and all(validation_dict.values()):
            return True

    def __get_generic_properties(self, event_name):
        with self.db_conn.cursor() as curs:
            curs.execute(f"""
                SELECT
                    DISTINCT event_property.name
                FROM 
                    event_property
                JOIN user_event on event_property.event_id = user_event.id
                WHERE user_event.name like '{event_name}'
                ORDER BY name;""")

            return [line for line in curs.fetchall()]

    def __validate_event(self, schema, schema_properties):
        for event in self.__get_events(event_name=schema[1]):
            for event_property in self.__get_event_properties(event_id=event[0]):
                schema_property = list(
                    filter(
                        lambda scp: self.clean_text(text=scp[0]) == self.clean_text(text=event_property[1]),
                        schema_properties
                    )
                )
                if schema_property:
                    if schema_property[0][1] == "varchar":
                        try:
                            str(event_property[2])
                        except ValueError:
                            self.update_invalid_user_event(event_id=event[0])
                            break
                    elif schema_property[0][1] == "numeric":
                        try:
                            float(event_property[2].replace(",", "."))
                        except ValueError:
                            self.update_invalid_user_event(event_id=event[0])
                            break
                    elif schema_property[0][1] == "date":
                        try:
                            datetime.fromisoformat(event_property[2])
                        except ValueError:
                            self.update_invalid_user_event(event_id=event[0])
                            break
                    elif schema_property[0][1] == "integer":
                        try:
                            int(event_property[2])
                        except ValueError:
                            self.update_invalid_user_event(event_id=event[0])
                            break
                else:
                    self.update_invalid_user_event(event_id=event[0])
                    break

            self.update_valid_user_event(event_id=event[0])

    def __get_events(self, event_name) -> List[Tuple[Any]]:
        with self.db_conn.cursor() as curs:
            curs.execute(f"""
                SELECT 
                    * 
                FROM 
                    user_event 
                WHERE name = '{event_name}' AND migrated = false 
                LIMIT 200;
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

    def update_invalid_user_event(self, event_id: int) -> None:
        with self.db_conn.cursor() as curs:
            curs.execute(f"UPDATE user_event SET valid ='unvalid' WHERE id={event_id};")
        self.db_conn.commit()

    def update_valid_user_event(self, event_id: int) -> None:
        with self.db_conn.cursor() as curs:
            curs.execute(f"UPDATE user_event SET valid ='validated' WHERE id={event_id};")
        self.db_conn.commit()

    @staticmethod
    def clean_text(text: str) -> str:
        text = unidecode.unidecode(
            text.replace("|", "")
            .replace(" ", "_")
            .replace("__", "_")
            .replace("___", "_")
        )
        return text.upper()


if __name__ == "__main__":
    validation = Validation()
    print(validation.execute())
