from typing import List, Tuple, Any, Dict
from data_base import main_db
from io import StringIO

import unidecode
import pandas as pd
import os

from psycopg2 import extras
import psycopg2


class Migration:
    def __init__(self) -> None:
        self.db_instance = main_db.DBInstance(public_key=os.environ["DEV_BACK"])
        self.buffer = StringIO()

    def execute(self):
        schemas: List[Tuple[Any]] = self.get_migrated_schemas()
        self.migrate_events(schemas=schemas)

    def migrate_events(self, schemas):
        conn = self.db_instance.make_conn(data=self.db_instance.get_conn_data())

        for schema in schemas:
            generic_properties = self.get_generic_properties(
                schema[1]
            )

            schema_properties = self.get_schema_properties(
                migrated_event_id=schema[0]
            )

            pivot = self.user_event_properties_crosstab(
                event_name=schema[1]
            )


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
    """

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

    def get_migrated_schemas(self) -> List[Tuple[Any]]:
        try:
            event_schemas = self.db_instance.handler(
                query="SELECT * FROM event_schema WHERE db_status = 'pending_create';"
            )
        except Exception as e:
            raise e
        else:
            return event_schemas

    @staticmethod
    def clean_text(text: str) -> str:
        text = unidecode.unidecode(
            text.replace("|", "").replace(" ", "_").replace("__", "_").replace("___", "_")
        )
        return text.upper()


    """

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
    """