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

    @staticmethod
    def clean_text(text: str) -> str:
        text = unidecode.unidecode(
            text.replace("|", "")
            .replace(" ", "_")
            .replace("__", "_")
            .replace("___", "_")
        )
        return text.lower()

    def handler(self):
        for distinct_name in self.get_distinct_user_event_names():
            if self.validate_event_schema(event_schema_name=distinct_name[0]):
                compare_properties = self.compare_properties(event_schema_name=distinct_name[0])
                if compare_properties:
                    self.update_event_schema_db_status(
                        event_schema_name=distinct_name[0],
                        db_status="alter_table_in_progress"
                    )
                    self.update_event_schema_properties(data={
                        'event_schema_id': self.get_event_schema_id(event_schema_name=distinct_name[0]),
                        'event_properties': compare_properties
                    })
                    self.update_event_schema_db_status(
                        event_schema_name=distinct_name[0],
                        db_status="create_completed"
                    )
            else:
                pass
                #  Create new record in event_schema with its properties "Create pending"

    def get_event_schema_id(self, event_schema_name):
        results = self.db.execute(
            f"select id from event_schema where name = '{event_schema_name}';"
        )
        for result in results:
            return result[0]

    def update_event_schema_properties(self, data):
        event_schema_id = data.get("event_schema_id")
        event_properties = data.get("event_properties")
        for event_property in event_properties:
            self.write_schema_property(event_property=event_property, event_schema_id=event_schema_id)

    def write_schema_property(self, event_property, event_schema_id):
        self.db.execute(
            f"""
                INSERT INTO 
                    property_event_schema(
                        name, type, updated_at, created_at, is_active, event_id)
                VALUES (
                        '{event_property[1]}', 
                        'varchar', 
                        '{datetime.now()}', 
                        '{datetime.now()}', 
                        true, 
                        {event_schema_id}
                        );
            """
        )

    def update_event_schema_db_status(self, event_schema_name, db_status):
        return self.db.execute(
            f"ALTER TABLE event_schema WHERE name = '{event_schema_name}' set db_status = '{db_status}';"
        )

    def compare_properties(self, event_schema_name):
        schema_properties_names = [
            sp[1]
            for sp in self.get_schema_properties_by_event_schema_name(
                event_schema_name=event_schema_name
            )
        ]
        result_properties = [
            ep
            for ep in self.get_event_properties_by_event_schema_name(
                event_schema_name=event_schema_name
            )
            if ep[1] in schema_properties_names
        ]
        return result_properties

    def get_schema_properties_by_event_schema_name(self, event_schema_name):
        return self.db.execute(f"""
            select 
                * 
            from 
                property_event_schema 
            where 
                event_id in (select id from event_schema where name = '{event_schema_name}');
        """)

    def get_event_properties_by_event_schema_name(self, event_schema_name):
        return self.db.execute(f"""
            select 
                * 
            from 
                event_property 
            where 
                event_id in (select id from user_event where name = '{event_schema_name}');
        """)

    def validate_event_schema(self, event_schema_name: str) -> bool:
        validate_column = self.db.execute(
            f"SELECT COUNT(1) FROM event_schema WHERE name = '{event_schema_name}';"
        )
        for result in validate_column:
            return result[0]
