from typing import List, Tuple, Any, Dict
from data_base import main_db
from io import StringIO

import pandas as pd
import os

from psycopg2 import extras
import psycopg2


class Main:
    def __init__(self) -> None:
        self.db_instance = main_db.DBInstance(public_key=os.environ["DEV_BACK"])
        self.buffer = StringIO()

    def execute(self):
        migrated_schemas: List[Tuple[Any]] = self.get_migrated_schemas()
        conn = self.db_instance.make_conn(data=self.db_instance.get_conn_data())

        for schema in migrated_schemas:
            event_id: int = self.get_event_id(name=schema[1])

            # Buscar los valores que no se le puedan hacer match en el  tipo de dato.
            """
            properties: pd.DataFrame = self.get_properties(event_id=event_id)

            properties_ids: List[int] = self.get_properties_ids(data=properties)

            data_insert: Dict[str, Any] = {
                "properties": properties,
                "conn": conn,
                "event_id": event_id
            }
            print(properties)
            """

            """
            try:
                self.insert_data(data=data_insert)
            except (Exception, psycopg2.DatabaseError   ) as error:
                logger.error(str(error))
                conn.rollback()
            finally:
                query = "DELETE FROM event_properties WHERE id = '%s'"
                extras.execute_values(
                    conn.cursor, query.as_string(conn.cursor), properties_ids
                )
                conn.commit()

                delete_event = self.delete_event(
                    event_id=self.get_event_id(name=schema[1])
                )
                conn.cursor.close()
            """

    def get_event_schema_properties(self, event_id: int) -> List[Tuple[Any]]:
        try:
            event_schemas_properties = self.db_instance.handler(
                query="SELECT id, name, type FROM property_event_schema WHERE event_id = event_id limit 100;"
            )
        except Exception as e:
            raise e
        else:
            return event_schemas_properties

    def get_pivot(self, event_id):
        columns = self.get_str_event_schema_properties(event_id=event_id)

        query_crostab = f"""select * from
                    (select * from crosstab('select event_id, name, value from event_property where name in
                    (select name from user_event where user_event.id ={event_id} limit 5000)') as ct({columns}))"""

        return query_crostab

    def get_str_event_schema_properties(self, event_id: int) -> str:
        event_schemas_properties = self.get_event_schema_properties(event_id=event_id)
        if event_schemas_properties:
            strings = [f'"{i[0]}" {i[1]}' for i in event_schemas_properties]
            ins = ",".join([str(i) for i in strings])
            return ins

    def get_migrated_schemas(self) -> List[Tuple[Any]]:
        try:
            event_schemas = self.db_instance.handler(
                query="SELECT * FROM event_schema WHERE db_status = 'pending_create';"
            )
        except Exception as e:
            raise e
        else:
            return event_schemas

    def get_user_events_count(self, name: str) -> int:
        user_events_count = self.db_instance.handler(
            query=f"SELECT COUNT(user_event) FROM user_event WHERE user_event.name = '{name}';"
        )
        return user_events_count[0][0]

    def get_event_id(self, name: str) -> int:
        # Propiedades de todos los eventos.
        # Count de las properties.
        try:
            event_id = self.db_instance.handler(
                query=f"SELECT id FROM user_event WHERE name='{name}' LIMIT 1;"
            )
        except Exception as e:
            raise e
        else:
            if event_id:
                return event_id[0][0]
            else:
                return 0

    def get_properties(self, event_id: int) -> pd.DataFrame:
        event_properties = self.db_instance.handler(
            query=f"SELECT id, name, value FROM event_property WHERE event_id='{event_id}';"
        )
        event_properties = pd.DataFrame(
            event_properties, columns=["id", "Name", "Value"]
        )
        return event_properties

    def get_properties_ids(self, data: pd.DataFrame) -> List[int]:
        col_one_list = data["id"].tolist()
        return col_one_list

    def charge_buffer_data(self, data: pd.DataFrame) -> None:
        data.to_csv(self.buffer, index_label="id", header=False)
        self.buffer.seek(0)

    def insert_data(self, data: Dict[str, Any]) -> None:
        self.charge_buffer_data(data["properties"])
        data["conn"].cursor.copy_from(self.buffer, "property_event_schema", sep=",")
        data["conn"].commit()

    def delete_event(self, event_id: int) -> List[Tuple[Any]]:
        try:
            delete_event_query = self.db_instance.handler(
                query=f"DELETE FROM user_event WHERE event_id='{event_id}';"
            )
        except Exception as e:
            raise e
        else:
            return delete_event_query


if __name__ == "__main__":
    main = Main()
    main.execute()
