from typing import List, Tuple, Dict, Any
from data_base import main_db
from sqlalchemy import create_engine

import unidecode
import pandas as pd
import psycopg2
import os


class Migration:
    def __init__(self) -> None:
        self.db_instance = main_db.DBInstance(public_key="kKS0DfTKpE8TqUZs")

    def execute(self) -> None:
        schemas: List[Tuple[Any]] = self.get_migrated_schemas()
        self.migrate_events(schemas=schemas)

    def migrate_events(self, schemas: List[Tuple[Any]]) -> None:
        for schema in schemas:
            schema_properties = self.get_schema_properties(migrated_event_id=schema[0])

            df_event_properties = self.df_event_properties(schema_name=schema[1])

            data = self.get_pivot_data(
                df_event_properties=df_event_properties,
                schema_properties=schema_properties,
                schema_name=schema[1],
            )

            self.insert_pivot(
                pivot=self.get_pivot_insert(data=data),
                schema_name=self.clean_text(text=schema[1]),
            )

            self.update_user_events_migrated(
                event_ids=df_event_properties["property_event_id"].tolist()
            )

    def get_pivot_insert(self, data: Dict[str, Any]) -> pd.DataFrame:
        pivot = self.get_data_frame(
            data=self.db_instance.handler(
                query=self.get_pivot_query(
                    columns=data.get("pivot_columns"),
                    schema_name=data.get("schema_name"),
                )
            ),
            columns=self.get_basic_pivot_columns() + data.get("name_columns"),
        )
        pivot.drop(self.get_delete_pivot_columns(), axis=1, inplace=True)
        return pivot

    def df_event_properties(self, schema_name: str) -> pd.DataFrame:
        df_event_properties = self.get_data_frame(
            data=self.join_user_event_properties(
                event_name=self.clean_text(text=schema_name)
            ),
            columns=self.get_event_properties_columns(),
        )
        return df_event_properties

    def get_pivot_data(
        self, df_event_properties, schema_properties, schema_name
    ) -> Dict[str, Any]:
        data = {
            "data_frame": df_event_properties,
            "pivot_columns": self.get_pivot_columns(
                schema_properties=schema_properties
            ),
            "schema_name": schema_name,
            "name_columns": [self.clean_text(gp[1]) for gp in schema_properties],
        }
        return data

    def get_pivot_columns(self, schema_properties) -> str:
        columns = "event_id integer, " + ", ".join(
            [f"{self.clean_text(gp[1])} varchar" for gp in schema_properties]
        )
        return columns

    @staticmethod
    def get_event_properties_columns() -> List[str]:
        return [
            "property_id",
            "property_event_id",
            "property_name",
            "property_value",
            "event_name",
            "event_created_at",
            "event_updated_at",
            "event_valid",
            "event_user_id",
        ]

    @staticmethod
    def get_basic_pivot_columns() -> List[str]:
        return [
            "id",
            "name",
            "value",
            "updated_at",
            "created_at",
            "user_id",
            "email",
            "migrated",
            "valid",
            "event_id",
        ]

    @staticmethod
    def get_delete_pivot_columns() -> List[str]:
        return ["name", "value", "email", "migrated", "valid", "event_id"]

    def insert_pivot(self, pivot: pd.DataFrame, schema_name: str):
        conn = create_engine(self.get_str_conn()).connect()

        conn1 = self.get_insert_conn()

        pivot.to_csv("pivot.csv")
        pivot.to_sql(schema_name, conn, if_exists="append", index=False)

        conn1.commit()
        conn1.close()

    @staticmethod
    def get_insert_conn():
        conn = psycopg2.connect(
            database="hey_elcolombiano",
            user="maiq",
            password="DevInstanceHey$",
            host="test-events-migration.csry9lg2mjjk.us-east-1.rds.amazonaws.com",
            port="5432",
        )
        conn.autocommit = True
        return conn

    @staticmethod
    def get_str_conn():
        user = "maiq"
        password = "DevInstanceHey$"
        host = "test-events-migration.csry9lg2mjjk.us-east-1.rds.amazonaws.com"
        name = "hey_elcolombiano"
        conn_string = f"postgresql://{user}:{password}@{host}/{name}"
        return conn_string

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
    def get_pivot_query(columns, schema_name: str) -> str:
        query = f"""
            SELECT * FROM user_event JOIN (SELECT *
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
                      AND valid = ''validated''
                    limit
                      1
                  )
                ORDER BY
                  user_event.id') as ct({columns})
            ) as prop on user_event.id=prop.event_id
        """
        return query

    def join_user_event_properties(self, event_name: str):
        try:
            user_events_properties = self.db_instance.handler(
                query=self.get_join_user_event_properties_query(event_name=event_name)
            )
        except Exception as e:
            raise e
        else:
            return user_events_properties

    @staticmethod
    def get_join_user_event_properties_query(event_name: str):
        query = f"""
            SELECT 
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
                      AND valid = 'validated'
                    limit 
                      1
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

    def get_schema_properties(self, migrated_event_id: int):
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
                query="SELECT * FROM event_schema WHERE db_status='pending_create' and name='SGC_SPEC';"
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
