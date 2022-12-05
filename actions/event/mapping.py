from datetime import datetime
from data_base import main_db
import unidecode


class Mapping():
    def __init__(self):
        self.db = main_db.DBInstance(public_key="***")

    def __get_distinct_user_event_names(self):
        try:
            distinct_names = self.db.handler(
                query="select distinct on (name) * from user_event;"
            )
        except Exception as e:
            raise e
        else:
            return distinct_names

    def __write_event_schema(self, event_schema):
        try:
            self.db.handler(
                query=f"""INSERT INTO public.event_schema(
                    name, updated_at, created_at, is_active, db_status, description, help_name, is_migrated)
	                    VALUES ({event_schema[1]}, {datetime.now()}, {datetime.now()}, true, '', ?, ?, ?);"""
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

    """    
    def handler(self):
        for distinct_name in self.__get_distinct_user_event_names():
    """
