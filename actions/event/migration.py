from typing import List, Tuple, Any
from data_base import main_db
from sqlalchemy import create_engine

import unidecode
import pandas as pd
import psycopg2


class Migration:
    def __init__(self) -> None:
        self.db_instance = main_db.DBInstance(public_key="kKS0DfTKpE8TqUZs")

    def execute(self) -> None:
        schemas: List[Tuple[Any]] = self.get_migrated_schemas()
        self.migrate_events(schemas=schemas)

    def migrate_events(self, schemas: List[Tuple[Any]]) -> None:
        for schema in schemas:
            schema_properties = self.get_schema_properties(migrated_event_id=schema[0])

            df_user_event_properties = self.get_data_frame(
                data=self.join_user_event_properties(
                    event_name=self.clean_text(text=schema[1])
                ),
                columns=[
                    "property_id",
                    "property_event_id",
                    "property_name",
                    "property_value",
                    "event_name",
                    "event_created_at",
                    "event_updated_at",
                    "event_valid",
                    "event_user_id",
                ],
            )

            event_ids = df_user_event_properties["property_event_id"].tolist()

            columns = "event_id integer, " + ", ".join(
                [f"{self.clean_text(gp[1])} varchar" for gp in schema_properties]
            )

            data = {
                "data_frame": df_user_event_properties,
                "pivot_columns": columns,
                "schema_name": schema[1],
                "name_columns": [self.clean_text(gp[1]) for gp in schema_properties],
            }

            query = self.get_pivot_query(
                columns=data.get("pivot_columns"),
                schema_name=data.get("schema_name"),
            )

            pivot_result = self.db_instance.handler(query=query)

            columns = [
                "id",
            ] + data.get("name_columns")

            pivot = self.get_data_frame(data=pivot_result, columns=columns)
            self.insert_pivot(pivot=pivot, schema_name=schema[1])
            self.update_events(event_ids=event_ids)

    def update_events(self, event_ids):
        pass

    @staticmethod
    def insert_pivot(pivot, schema_name):

        conn_string = "postgresql://postgres:*123@127.0.0.1/Testing"

        db = create_engine(conn_string)

        conn = db.connect()

        conn1 = psycopg2.connect(
            database="Testing",
            user="postgres",
            password="*123",
            host="127.0.0.1",
            port="5432",
        )

        conn1.autocommit = True
        cursor = conn1.cursor()

        pivot_lists = list(pivot.columns.values)

        data = pivot.to_csv("insert_testing.csv", index=False)

        data = data[pivot_lists]

        data.to_sql(schema_name, conn, if_exists="replace")

        sql1 = """select * from books;"""
        cursor.execute(sql1)

        conn1.commit()
        conn1.close()

    def update_user_events_migrated(self, event_ids):
        for event_id in event_ids:
            try:
                self.db_instance.handler(
                    query=f"""UPDATE user_event SET migrated=true WHERE id={event_id};"""
                )
            except Exception as e:
                raise e
            else:
                continue

    @staticmethod
    def get_pivot_query(columns, schema_name) -> str:
        query = f"""SELECT *
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
                            name = ''{schema_name}''
                            AND migrated = false
                        limit
                            5
                        )
                    ORDER BY
                        user_event.id') as ct({columns})"""
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
                query="SELECT * FROM event_schema WHERE db_status='pending_create' and name='NAV_DMP';"
            )
        except Exception as e:
            raise e
        else:
            return event_schemas

    @staticmethod
    def clean_text(text: str) -> str:
        text = unidecode.unidecode(
            text.replace("|", "")
            .replace(" ", "_")
            .replace("__", "_")
            .replace("___", "_")
        )
        return text.upper()

    @staticmethod
    def get_data_frame(data: List[Tuple[Any]], columns: List[str]) -> pd.DataFrame:
        event_properties = pd.DataFrame(data, columns=columns)
        return event_properties
