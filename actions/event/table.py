from typing import Any, List, Tuple
from data_base import main_db
import unidecode


class Table:
    def __init__(self, name: str):
        self.name = self.clean_name(name=name)
        self.conn = main_db.DBInstance(public_key="kKS0DfTKpE8TqUZs")

    def build_table(self) -> str:
        if self.validate_table():
            self.update_table()
            return f"Updated table with name {self.name}"
        else:
            self.create_table()
            return f"Created table with name {self.name}"

    def create_table(self) -> None:
        line_fields = ", ".join(self.get_creation_fields())
        table = f"CREATE TABLE {self.name} ({line_fields});"
        self.conn.handler(query=table)

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
                            WHERE COLUMN_NAME='{column_name}' AND table_name='{table_name}'
                        );
                """
        return query

    def validate_column(self, table_name: str, column_name: str) -> bool:
        validate_column = self.conn.handler(
            query=self.get_validate_column_query(
                table_name=table_name, column_name=self.clean_name(name=column_name)
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
        return ['id integer PRIMARY KEY', 'updated_at date', 'created_at date', 'user_id integer']

    @staticmethod
    def clean_name(name: str) -> str:
        text = unidecode.unidecode(
            name.replace("|", "")
                .replace(" ", "_")
                .replace("__", "_")
                .replace("___", "_")
        )
        return text.lower()

    def get_varchar_line(self, column: Tuple[Any]) -> str:
        return f"{self.clean_name(name=column[0])} {column[1]} 255"

    def get_normal_line(self, column: Tuple[Any]) -> str:
        return f"{self.clean_name(name=column[0])} {column[1]}"
