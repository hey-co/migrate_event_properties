from typing import List, Tuple, Any, Dict
from data_base import main_db
from io import StringIO

import unidecode
import pandas as pd
import os

# from psycopg2 import extras
# import psycopg2


class Migration:
    def __init__(self) -> None:
        self.db_instance = main_db.DBInstance(public_key=os.environ["DEV_BACK"])
        self.buffer = StringIO()

    def execute(self) -> None:
        schemas: List[Tuple[Any]] = self.get_migrated_schemas()
        self.migrate_events(schemas=schemas)

    def migrate_events(self, schemas: List[Tuple[Any]]) -> None:
        data: Dict[Any] = self.__get_data(schemas=schemas)
        self.build_pivot(data=data)

    def __get_data(self, schemas) -> Dict[Any]:
        for schema in schemas:
            """
            generic_properties = self.get_generic_properties(
                schema[1]
            )
            """

            schema_properties = self.get_schema_properties(
                migrated_event_id=schema[0]
            )

            df_user_event_properties = self.get_data_frame(
                data=self.join_user_event_properties(
                    event_name=schema[1]
                )
            )

            columns = [f"{self.clean_text(gp[1])}, {gp[2]}" for gp in schema_properties]

            data = {
                "data_frame": df_user_event_properties,
                "columns": columns,
                "schema_name": schema[1]
            }
            return data

    def build_pivot(self, data):
        self.get_pivot_query(
            columns=data.get("columns"),
            schema_name=data.get("schema_name")
        )

    @staticmethod
    def get_pivot_query(columns, schema_name) -> str:
        query = f"""select * from user_event join (SELECT *
                    FROM crosstab('
                        SELECT
                          user_event.id,
                          event_property.name,
                          event_property.value
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
                              name = '{schema_name}'
                              AND migrated = false
                            limit
                              5
                          )
                        ORDER BY
                          user_event.id') as ct({columns})
                ) as prop on user_event.id=prop.event_id"""
        return query

    def join_user_event_properties(self, event_name):
        try:
            user_events_properties = self.db_instance.handler(
                query=self.get_join_user_event_properties_query(event_name=event_name)
            )
        except Exception as e:
            raise e
        else:
            return user_events_properties

    @staticmethod
    def get_join_user_event_properties_query(event_name):
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
                              5000
                          ) 
                        ORDER BY 
                          user_event.id;
                    """
        return query

        """
            conn = self.db_instance.make_conn(data=self.db_instance.get_conn_data())
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

    @staticmethod
    def get_data_frame(data: List[Tuple[Any]]) -> pd.DataFrame:
        event_properties = pd.DataFrame(
            data, columns=["id", "Name", "Value"]
        )
        return event_properties

    """
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
