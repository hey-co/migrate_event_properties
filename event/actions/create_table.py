import os
from typing import Any, List, Tuple

from dotenv import load_dotenv
import multitenancy
load_dotenv()


class Table:
    def __init__(self, event):
        self.name = event.get("schema_name")
        self.conn = self.get_session_db(private_key=event.get("private_key"))

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

    def build_table(self) -> str:
        if self.validate_table():
            self.update_table()
            return f"Updated table with name {self.name}"
        else:
            self.create_table()
            return f"Created table with name {self.name}"

    def create_table(self) -> None:
        line_fields = ", ".join(self.get_creation_fields())
        query_table = f"CREATE TABLE {self.name} ({line_fields});"
        self.conn.handler(query=query_table)

    def get_creation_fields(self) -> List[str]:
        line_fields = []
        for c in self.get_columns():
            if c[1] == 'varchar':
                line_field = self.get_varchar_line(column=c)
            else:
                line_field = self.get_normal_line(column=c)
            line_fields.append(line_field)
        return line_fields + self.get_base_fields()

    def update_table(self) -> None:
        for c in self.get_columns():
            if not self.validate_column(table_name=self.name, column_name=c[0]):
                self.update_field(column=c)

    def update_field(self, column: Tuple[Any]) -> None:
        if column[1] == "varchar":
            line = self.get_varchar_line(column=column)
        else:
            line = self.get_normal_line(column=column)
        self.conn.handler(query=f"ALTER TABLE {self.name} ADD COLUMN {line};")

    def validate_table(self) -> bool:
        validate_table = self.conn.handler(query=self.get_validate_table_query())
        return validate_table[0][0]

    def get_validate_table_query(self) -> str:
        query = f"""SELECT 
                        EXISTS(
                            SELECT 
                            FROM 
                                information_schema.tables 
                            WHERE table_schema = 'public' AND table_name = '{self.name}'
                        );
                """
        return query

    @staticmethod
    def get_validate_column_query(table_name: str, column_name: str) -> str:
        query = f"""SELECT 
                        EXISTS(
                            SELECT 
                            FROM 
                                information_schema.columns 
                            WHERE 
                                table_schema = 'public' 
                                AND COLUMN_NAME='{column_name}' 
                                AND table_name='{table_name}' 
                        );
                """
        return query

    def validate_column(self, table_name: str, column_name: str) -> bool:
        validate_column = self.conn.handler(
            query=self.get_validate_column_query(
                table_name=table_name, column_name=column_name
            )
        )
        return validate_column[0][0]

    def get_columns(self) -> List[Tuple[Any]]:
        columns = self.conn.handler(query=f"""
            SELECT
                property_event_schema.name,
                property_event_schema.type
            FROM
                property_event_schema
                INNER JOIN event_schema ON property_event_schema.event_id = event_schema.id
            WHERE
                event_schema.name='{self.name}'
            ORDER BY
                property_event_schema.name;
        """)
        return columns

    @staticmethod
    def get_base_fields() -> List[str]:
        return [
            'id integer PRIMARY KEY',
            'updated_at date',
            'created_at date',
            'user_id integer',
            'integration_id integer',
            'integration_type varchar(100)',
            'uuid varchar(40)'
        ]

    @staticmethod
    def get_varchar_line(column: Tuple[Any]) -> str:
        return f"{column[0]} {column[1]}(255)"

    @staticmethod
    def get_normal_line(column: Tuple[Any]) -> str:
        return f"{column[0]} {column[1]}"


def lambda_handler(event, context):
    return Table(event=event).build_table()
