from typing import List, Tuple, Any, Dict
from data_base import main_db
from io import StringIO

import pandas as pd
import os

import logging

logger = logging.getLogger()

from psycopg2 import extras
import psycopg2


class Main:
    def __init__(self) -> None:
        self.db_instance = main_db.DBInstance(public_key=os.environ["ELCOLOMBIANO"])
        self.buffer = StringIO()

    def execute(self):
        migrated_schemas: List[Tuple[Any]] = self.get_migrated_schemas()
        conn = self.db_instance.make_conn(data=self.db_instance.get_conn_data())

        for schema in migrated_schemas:
            # while self.get_user_events_count(name=schema[1]) > 5000:
            event_id: int = self.get_event_id(name=schema[1])

            properties: pd.DataFrame = self.get_properties(event_id=event_id)

            data_insert: Dict[str, Any] = {
                "properties": properties,
                "conn": conn,
                "event_id": event_id
            }

            properties_ids: List[int] = self.get_properties_ids(data=properties)

            try:
                self.insert_data(data=data_insert)
            except (Exception, psycopg2.DatabaseError) as error:
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

    def get_migrated_schemas(self) -> List[Tuple[Any]]:
        event_schemas = self.db_instance.handler(
            query="SELECT * FROM event_schema WHERE db_status = 'pending_create';"
        )
        return event_schemas

    def get_user_events_count(self, name: str) -> int:
        user_events_count = self.db_instance.handler(
            query=f"SELECT COUNT(user_event) FROM user_event WHERE user_event.name = '{name}';"
        )
        return user_events_count[0][0]

    def get_event_id(self, name: str) -> int:
        event_id = self.db_instance.handler(
            query=f"SELECT id FROM user_event WHERE name='{name}' LIMIT 1;"
        )
        return event_id[0][0]

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
        delete_event_query = self.db_instance.handler(
            query=f"DELETE FROM user_event WHERE event_id='{event_id}';"
        )
        return delete_event_query


if __name__ == "__main__":
    main = Main()
    main.execute()
