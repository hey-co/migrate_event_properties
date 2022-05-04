from data_base import main_db
from psycopg2 import extras
from io import StringIO

import pandas as pd
import psycopg2


class Handler:
    def __init__(self):
        self.db_instance = main_db.DBInstance(public_key="aVFr8UuELcKw4BVh")
        self.buffer = StringIO()


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
        event_properties = pd.DataFrame(event_properties, columns=['id', 'Name', 'Value'])
        return event_properties

    def get_properties_ids(self, data):
        col_one_list = data['id'].tolist()
        return col_one_list

    def charge_buffer_data(self, data):
        data.to_csv(handler.buffer, index_label='id', header=False)
        self.buffer.seek(0)

    def insert_data(self, data):
        self.charge_buffer_data(data)
        conn = self.db_instance.make_conn(data=self.db_instance.get_conn_data())
        conn.cursor.copy_from(self.buffer, schema.name, sep=",")
        conn.commit()

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

            event_id = handler.get_event_id(
                name=schema.name
            )

            properties = handler.get_properties(
                event_id=event_id
            )

            properties_ids = handler.get_properties_ids(
                data=properties
            )

            try:
                handler.insert_data()
            except (Exception, psycopg2.DatabaseError) as error:
                print("Error: %s" % error)
                conn.rollback()
            finally:
                query = "DELETE FROM event_properties WHERE id = '%s'"
                extras.execute_values(conn.cursor, query.as_string(conn.cursor), properties_ids)
                conn.commit()

                delete_event = handler.delete_event(event_id=event_id)
                conn.cursor.close()