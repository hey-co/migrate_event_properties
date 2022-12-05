from datetime import datetime
import os

from dotenv import load_dotenv
import unidecode

import multitenancy
load_dotenv()


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
            results = self.db.execute(
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
        else:
            self.db.commit()
            for result in results:
                self.update_event_property(event_name=event_schema_name, event_schema_id=result[0])
                return result[0]

    def update_event_property(self, event_name, event_schema_id):
        event_properties = self.get_event_properties(
            user_events=self.get_events_by_name(event_name=event_name)
        )
        self.move_event_properties(event_properties=event_properties, event_schema_id=event_schema_id)

    def move_event_properties(self, event_properties, event_schema_id):
        for event_property in event_properties:
            query = f"""INSERT INTO property_event_schema(
                        name, type, updated_at, created_at, is_active, event_id)
                        VALUES ('{event_property[1]}', '{event_property[0]}', '{datetime.now()}', '{datetime.now()}', true, {event_schema_id});"""
        self.db.execute(query)

    def get_events_by_name(self, event_name):
        return self.db.execute(f"select * from user_event where name = '{event_name}';")

    def get_event_properties(self, user_events):
        return self.db.execute(f"select * from event_property where event_name = '{event_name}';")

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
            if not self.validate_column(table_name='event_schema', column_name=distinct_name[0]):
                self.write_event_schema(event_schema_name=distinct_name[0])

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
        for result in validate_column:
            return result[0]
