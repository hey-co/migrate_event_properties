from typing import List, Tuple, Any
from data_base import validation_conn
from datetime import datetime
import os
import unidecode


class Validation:
    def __init__(self) -> None:
        self.db_conn = validation_conn.DBInstance(public_key="kKS0DfTKpE8TqUZs").make_conn()

    def execute(self):
        migrated_schemas: List[Tuple[Any]] = self.__get_migrated_schemas()
        print(migrated_schemas)
        #self.validate_events(migrated_schemas=migrated_schemas)

    def __get_migrated_schemas(self) -> List[Tuple[Any]]:
        with self.db_conn.cursor() as curs:
            curs.execute("SELECT * FROM event_schema WHERE db_status = 'pending_create' AND name='SGC_SPEC';")
            return [line for line in curs.fetchall()]

    def validate_events(self, migrated_schemas):
        for schema in migrated_schemas:
            self.__validate_event(schema=schema)

        self.db_conn.close()

    def __validate_event(self, schema):
        for event in self.__get_user_events(event_name=schema[1]):
            for event_property in self.__event_properties(event_id=event[0]):
                schema_property = self.__get_schema_property(event_property[1])
                if schema_property[2] == "text":
                    try:
                        cast = str(event_property[2])
                    except:
                        self.update_invalid_user_event(event_id=event_property[1])
                        break
                    else:
                        if isinstance(cast, str):
                            self.update_valid_user_event(event_id=event_property[1])

                elif schema_property[2] == "numeric":
                    try:
                        cast = int(event_property[2])
                    except:
                        self.update_invalid_user_event(event_id=event_property[1])
                        break
                    else:
                        if isinstance(cast, int):
                            self.update_valid_user_event(event_id=event_property[1])

                elif schema_property[2] == "date":
                    try:
                        cast = datetime.strptime(event_property[2], '%d/%m/%y')
                    except:
                        self.update_invalid_user_event(event_id=event_property[1])
                        break
                    else:
                        if isinstance(cast, datetime):
                            self.update_valid_user_event(event_id=event_property[1])

    def __get_user_events(self):
        with self.db_conn.cursor() as curs:
            curs.execute("")



    def get_generic_properties(self, name_event: str) -> List[Tuple[Any]]:
        try:
            generic_properties = self.db_instance.handler(
                query=f"""select name, count(*) from event_property where event_id in 
                    (select id from user_event where name='{name_event}') group by name"""
            )
        except Exception as e:
            raise e
        else:
            return generic_properties

    def get_schema_properties(self, migrated_event_id):
        try:
            event_properties = self.db_instance.handler(
                query=f"select * from property_event_schema where event_id={migrated_event_id};"
            )
        except Exception as e:
            raise e
        else:
            return event_properties

    def join_user_event_properties(self, event_name):
        try:
            user_events_properties = self.db_instance.handler(
                query=self.get_user_event_properties_query(event_name=event_name)
            )
        except Exception as e:
            raise e
        else:
            return user_events_properties

    @staticmethod
    def get_user_event_properties_query(event_name):
        query = f"""SELECT 
                          event_property.id, 
                          event_property.event_id, 
                          event_property.name, 
                          event_property.value, 
                          user_event.name, 
                          user_event.created_at, 
                          user_event.updated_at, 
                          user_event.valid, 
                          user_event.user_id 
                        FROM 
                          event_property 
                          INNER JOIN user_event ON event_property.event_id = user_event.id 
                        WHERE 
                          user_event.id in (
                            SELECT 
                              id 
                            FROM 
                              user_event 
                            WHERE 
                              name = '{event_name}' 
                              AND migrated = false 
                            limit 
                              100
                          ) 
                        ORDER BY 
                          user_event.id;
                    """
        return query

    def update_invalid_user_event(self, event_id):
        try:
            update_user_event = self.db_instance.handler(
                query=f"UPDATE user_event SET valid ='unvalid' WHERE id={event_id};"
            )
        except Exception as e:
            raise e
        else:
            return update_user_event

    def update_valid_user_event(self, event_id):
        try:
            update_user_event = self.db_instance.handler(
                query=f"UPDATE user_event SET valid ='validated' WHERE id={event_id};"
            )
        except Exception as e:
            raise e
        else:
            return update_user_event

    @staticmethod
    def clean_text(text: str) -> str:
        text = unidecode.unidecode(
            text.replace("|", "")
            .replace(" ", "_")
            .replace("__", "_")
            .replace("___", "_")
        )
        return text.lower()



if __name__ == "__main__":
    validation = Validation()
    print(validation.execute())
