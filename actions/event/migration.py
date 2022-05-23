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

    def get_user_events_properties(self, event_name):
        try:
            user_events_properties = self.db_instance.handler(
                query=self.get_user_events_properties_query(event_name=event_name)
            )
        except Exception as e:
            raise e
        else:
            return user_events_properties

    @staticmethod
    def get_user_events_properties_query(event_name):
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
                          AND valid='true'
                        limit 
                          5000
                      ) 
                    ORDER BY 
                      user_event.id;
                """
        return query

    @staticmethod
    def get_event_names(cleaned_names, generic_properties):
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

        return event_names_1 + event_names_2

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
    def clean_name_properties(name: str) -> str:
        name = unidecode.unidecode(
            name.replace("|", "").replace(" ", "_").replace("__", "_")
        )
        return name.upper()

    #########################################################################################################

    def get_pivot(self, event_id):
        columns = self.get_str_event_schema_properties(event_id=event_id)

        query_crostab = f"""select * from
                    (select * from crosstab('select event_id, name, value from event_property where name in
                    (select name from user_event where user_event.id ={event_id} limit 5000)') as ct({columns}))"""

        return query_crostab

    """
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
    """