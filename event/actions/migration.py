import time
from typing import List, Tuple, Dict, Any
from sqlalchemy import create_engine

from dotenv import load_dotenv
import pandas as pd
import psycopg2
import multitenancy
import os

from event import schemas as validators

load_dotenv()


class Migration:
    def __init__(self, event) -> None:
        self.db_instance = self.get_session_db(private_key=event.get("private_key"))
        self.event_schema_name = event.get("schema_name")

    @staticmethod
    def get_session_db(private_key: str = None):
        dict_db_tenant = {
            "db_name": os.environ.get("MULTI_TENANCY_DB_NAME"),
            "db_user": os.environ.get("MULTI_TENANCY_DB_USER"),
            "db_password": os.environ.get("MULTI_TENANCY_DB_PASSWORD"),
            "db_port": os.environ.get("MULTI_TENANCY_DB_PORT"),
            "db_host": os.environ.get("MULTI_TENANCY_DB_HOST"),
        }
        return multitenancy.get_session_by_secret_key(private_key, dict_db_tenant)

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
            data=self.db_instance.execute(self.get_pivot_query(
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
    def get_pivot_columns(schema_properties) -> str:
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
            "integration_id",
            "integration_type",
            "uuid",
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
            database=os.environ.get("MULTI_TENANCY_CLIENT_DB_NAME"),
            user=os.environ.get("MULTI_TENANCY_DB_USER"),
            password=os.environ.get("MULTI_TENANCY_DB_PASSWORD"),
            host=os.environ.get("MULTI_TENANCY_DB_HOST"),
            port=os.environ.get("MULTI_TENANCY_DB_PORT"),
        )
        conn.autocommit = True
        return conn

    @staticmethod
    def get_str_conn():
        user = os.environ.get("MULTI_TENANCY_DB_USER")
        password = os.environ.get("MULTI_TENANCY_DB_PASSWORD")
        host = os.environ.get("MULTI_TENANCY_DB_HOST")
        name = os.environ.get("MULTI_TENANCY_CLIENT_DB_NAME")
        conn_string = f"postgresql://{user}:{password}@{host}/{name}"
        return conn_string

    def update_user_events_migrated(self, event_ids):
        try:
            self.db_instance.execute(
                f"UPDATE user_event SET migrated=true WHERE id in ({', '.join(map(str, event_ids))});"
            )
        except Exception as e:
            raise e
        else:
            self.db_instance.commit()

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
                      RIGHT JOIN user_event ON event_property.event_id = user_event.id
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
                          100000
                      )', '{generic_properties}') as ct({columns})
            ) as prop on user_event.id=prop.event_id
        """
        return query

    @staticmethod
    def __get_generic_properties(schema_properties):
        generic_properties_query = f"""
            SELECT a
            FROM (
                values {", ".join([f"(''{sp[1].upper()}'')" for sp in schema_properties])}
            ) s(a);
        """
        return generic_properties_query

    def get_schema_properties(self, migrated_event_id: int):
        try:
            event_properties = self.db_instance.execute(
                f"select * from property_event_schema where event_id={migrated_event_id} ORDER BY name ASC;"
            )
        except Exception as e:
            raise e
        else:
            return [ep for ep in event_properties]

    def get_migrated_schemas(self) -> List[Tuple[Any]]:
        try:
            event_schemas = self.db_instance.execute(
                f"""
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
    return Migration(event=event).execute()
