from typing import List, Tuple, Dict, Any
from data_base import main_db
from sqlalchemy import create_engine

import pandas as pd
import psycopg2
import os

from event import schemas as validators


class Migration:
    def __init__(self, event) -> None:
        self.db_instance = main_db.DBInstance(public_key="kKS0DfTKpE8TqUZs")
        self.event_schema_name = event.get("schema_name")

    def execute(self) -> None:
        schemas: List[Tuple[Any]] = self.get_migrated_schemas()
        self.migrate_events(schemas=schemas)

    def migrate_events(self, schemas: List[Tuple[Any]]) -> None:
        for schema in schemas:
            schema_properties = self.get_schema_properties(migrated_event_id=schema[0])

            data = self.get_pivot_data(
                schema_properties=schema_properties, schema_name=schema[1]
            )

            self.insert_pivot(
                pivot=self.get_pivot_insert(data=data),
                schema_name=schema[1],
            )

    def get_pivot_insert(self, data: Dict[str, Any]) -> pd.DataFrame:
        pivot = self.get_data_frame(
            data=self.db_instance.handler(
                query=self.get_pivot_query(
                    columns=data.get("pivot_columns"),
                    schema_name=data.get("schema_name"),
                    generic_properties=data.get("generic_properties")
                )
            ),
            columns=self.get_basic_pivot_columns() + data.get("name_columns"),
        )

        pivot.drop(self.get_delete_pivot_columns(), axis=1, inplace=True)

        return pivot

    def get_pivot_data(self, schema_properties, schema_name) -> Dict[str, Any]:
        data = {
            "pivot_columns": self.get_pivot_columns(
                schema_properties=schema_properties
            ),
            "schema_name": schema_name,
            "generic_properties": self.__get_generic_properties(schema_properties=schema_properties),
            "name_columns": [gp[1] for gp in schema_properties],
        }
        return data

    @staticmethod
    def get_pivot_columns(self, schema_properties) -> str:
        columns = "event_id integer, " + ", ".join(
            [f'"{gp[1]}" {gp[2]}' for gp in schema_properties]
        )
        return columns

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
        conn1 = self.get_insert_conn()
        pivot.to_sql(schema_name, create_engine(self.get_str_conn()).connect(), if_exists="append", index=False)
        self.update_user_events_migrated(event_ids=list(pivot["id"]))
        conn1.commit()
        conn1.close()

    @staticmethod
    def get_insert_conn():
        conn = psycopg2.connect(
            database="hey_elcolombiano",
            user="maiq",
            password="qwerty123.",
            host="migration-testing.ch3slpycdtsd.us-east-1.rds.amazonaws.com",
            port="5432",
        )
        conn.autocommit = True
        return conn

    @staticmethod
    def get_str_conn():
        user = "maiq"
        password = "qwerty123."
        host = "migration-testing.ch3slpycdtsd.us-east-1.rds.amazonaws.com"
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
    def get_pivot_query(columns, schema_name: str, generic_properties) -> str:
        query = f"""
            SELECT * FROM user_event JOIN (SELECT *
                FROM crosstab('
                    SELECT
                      user_event.id as event_id,
                      event_property.name as name,
                      event_property.value as value
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
                          name = ''{schema_name.upper()}''
                          AND migrated = false
                          AND valid = ''validated''
                        LIMIT
                          10000
                      )', '{generic_properties}') as ct({columns})
            ) as prop on user_event.id=prop.event_id
        """
        return query

    @staticmethod
    def __get_generic_properties(self, schema_properties):
        generic_properties_query = f"""
            SELECT a
            FROM (
                values {", ".join([f"(''{sp[1].upper()}'')" for sp in schema_properties])}
            ) s(a);
        """
        return generic_properties_query

    def get_schema_properties(self, migrated_event_id: int):
        try:
            event_properties = self.db_instance.handler(
                query=f"select * from property_event_schema where event_id={migrated_event_id} ORDER BY name ASC;"
            )
        except Exception as e:
            raise e
        else:
            return event_properties

    def get_migrated_schemas(self) -> List[Tuple[Any]]:
        try:
            event_schemas = self.db_instance.handler(
                query=f"""
                    SELECT 
                        * 
                    FROM 
                        event_schema 
                    WHERE 
                        db_status='{validators.SqlStructureDbStatus.CREATE_PENDING}' and name='{self.event_schema_name}';
                    """
            )
        except Exception as e:
            raise e
        else:
            return event_schemas

    @staticmethod
    def get_data_frame(data: List[Tuple[Any]], columns: List[str]) -> pd.DataFrame:
        event_properties = pd.DataFrame(data, columns=columns)
        return event_properties


def lambda_handler(event, context):
    return Migration().execute(event=event)


if __name__ == '__main__':
    lambda_handler(
        event={
            "schema_name": "sgc_spec"
        },
        context={}
    )