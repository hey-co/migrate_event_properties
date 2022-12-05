from datetime import datetime
import os

from dotenv import load_dotenv
import unidecode

import multitenancy


class Mapping:
    def __init__(self, event):
        self.db = self.get_session_db(private_key=event.get("private_key"))

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

    def get_distinct_user_event_names(self):

        try:
            distinct_names = self.db.execute("select distinct on (name) name from user_event;")
        except Exception as e:
            raise e
        else:
            return distinct_names

    def write_event_schema(self, event_schema_name):
        try:
            self.db.execute(
                f"""INSERT INTO event_schema(name, updated_at, created_at, is_active, db_status, is_migrated)
                            VALUES (
                                '{self.clean_text(text=event_schema_name)}', 
                                '{datetime.now()}', 
                                '{datetime.now()}', 
                                true, 
                                'pending_update', 
                                false
                            );
                """
            )
        except Exception as e:
            raise e

    @staticmethod
    def clean_text(text: str) -> str:
        text = unidecode.unidecode(
            text.replace("|", "")
            .replace(" ", "_")
            .replace("__", "_")
            .replace("___", "_")
        )
        return text.lower()

    def update_unique_schemas(self):
        for distinct_name in self.get_distinct_user_event_names():
            if not self.validate_column(table_name='event_schema', column_name=distinct_name):
                self.write_event_schema(event_schema_name=distinct_name)

    def handler(self):
        try:
            self.update_unique_schemas()
        except Exception as e:
            raise e

    @staticmethod
    def get_validate_column_query(table_name: str, column_name: str) -> str:
        query = f"""SELECT 
                        EXISTS(
                            SELECT 
                            FROM 
                                information_schema.columns 
                            WHERE 
                                table_schema = 'public' 
                                AND COLUMN_NAME='{column_name.lower()}' 
                                AND table_name='{table_name.lower()}' 
                        );
                """
        return query

    def validate_column(self, table_name: str, column_name: str) -> bool:
        validate_column = self.db.execute(
            self.get_validate_column_query(
                table_name=table_name, column_name=self.clean_text(text=column_name)
            )
        )
        return validate_column
