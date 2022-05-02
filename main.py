from data_base import main_db
import pandas as pd
from io import StringIO

class Handler:
    def __init__(self):
        self.db_instance = main_db.DBInstance(public_key="aVFr8UuELcKw4BVh")

    def get_migrated_schemas(self, query=""):
        event_schemas = self.db_instance.handler(
            query="SELECT * FROM event_schema WHERE db_status = 'migrated';"
        )
        return event_schemas

    def get_user_events_count(self, name):
        user_events_count = self.db_instance.handler(
            query=f"SELECT COUNT(user_event) FROM user_event WHERE user_event.name = '{name}';"
        )
        return user_events_count

    def get_event_id(self, name):
        event_id = self.db_instance.handler(
            query=f"SELECT id FROM user_event WHERE name='{name}';"
        )
        return event_id

    def get_properties(self, event_id):
        event_properties = self.db_instance.handler(
            query=f"SELECT name, value FROM even_property WHERE event_id='{event_id}';"
        )
        return event_properties


if __name__ == '__main__':
    handler = Handler()

    migrated_schemas = handler.get_migrated_schemas()

    for schema in migrated_schemas:
        while handler.user_events_count(name=schema.name) > 5000:
            event_id = handler.get_event_id(name=schema.name)
            properties = handler.get_properties(event_id=event_id)

            df = pd.DataFrame(properties, columns=['Name', 'Value'])

            buffer = StringIO()
            df.to_csv(buffer, index_label='id', header=False)
            buffer.seek(0)

            cursor = conn.cursor()
            try:
                cursor.copy_from(buffer, table, sep=",")
                conn.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                print("Error: %s" % error)
                conn.rollback()
                cursor.close()

        # delete
        # event_generic
        # ids
        # inserted in event_x