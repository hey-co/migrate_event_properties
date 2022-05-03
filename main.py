from data_base import main_db
from psycopg2 import extras
from io import StringIO

import pandas as pd
import psycopg2


class Handler:
    def __init__(self):
        self.db_instance = main_db.DBInstance(public_key="aVFr8UuELcKw4BVh")

    def get_migrated_schemas(self):
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
            query=f"SELECT id, name, value FROM even_property WHERE event_id='{event_id}';"
        )
        return event_properties

    def delete_event(self, event_id):
        delete_event_query = self.db_instance.handler(
            query=f"DELETE FROM user_event WHERE event_id='{event_id}';"
        )
        return delete_event_query


if __name__ == '__main__':
    handler = Handler()

    migrated_schemas = handler.get_migrated_schemas()

    for schema in migrated_schemas:
        while handler.get_user_events_count(name=schema.name) > 5000:
            event_id = handler.get_event_id(name=schema.name)
            properties = handler.get_properties(event_id=event_id)
            properties_id = [i[0] for i in properties]

            df = pd.DataFrame(properties, columns=['id', 'Name', 'Value'])

            buffer = StringIO()
            df.to_csv(buffer, index_label='id', header=False)
            buffer.seek(0)

            conn_data = handler.db_instance.get_conn_data()
            conn = handler.db_instance.make_conn(data=conn_data)

            try:
                conn.cursor.copy_from(buffer, schema.name, sep=",")
                conn.commit()
            except (Exception, psycopg2.DatabaseError) as error:
                print("Error: %s" % error)
                conn.rollback()
            finally:
                query = "DELETE FROM event_properties WHERE id = '%s'"
                extras.execute_values(conn.cursor, query.as_string(conn.cursor), properties_id)
                conn.commit()

                delete_event = handler.delete_event(event_id=event_id)
                conn.cursor.close()