from data_base import main_db
from typing import List, Tuple, Any
import re


class Clean:
    def __init__(self, event_name: str, property_name: str, public_key: str):
        self.event_name = event_name
        self.property_name = property_name
        self.properties = self.__get_event_properties()
        self.db_instance = main_db.DBInstance(public_key=public_key)

    def get_clean_data_query(self, old_value: str, new_value: str) -> str:
        query: str = f"""
            UPDATE
                event_property
            SET
                value = regexp_replace(value, '{old_value}', '{new_value}')
            WHERE
                event_id in (select id from user_event where name = '{self.event_name}' LIMIT 200) and name
            LIKE
                '{self.property_name}';
            """
        return query

    def __get_event_properties(self) -> List[Tuple[Any]]:
        property_values = self.db_instance.handler(query=f"""
            SELECT 
                property_id, value 
            FROM 
                event_property 
            WHERE 
                event_id in (
                    select id from user_event where name = '{self.event_name}' LIMIT 200
                ) 
            AND 
                name LIKE '{self.property_name}';
            """)
        return property_values

    def cast_integer(self):
        for event_property in self.properties:
            self.db_instance.handler(query=f"""
                UPDATE
                    event_property            
                WHERE
                    id = {event_property[0]}
                SET
                    value = CAST(({event_property[2]}) AS int)
            """)

    def validate_email(self):
        result = {}
        for ep in self.properties:
            if re.match(r"[^@]+@[^@]+\.[^@]+", ep[2]):
                result[ep[2]] = True
            else:
                result[ep[2]] = False

    def change_properties_name(self, new_name: str):
        for event_property in self.properties:
            self.db_instance.handler(query=f"""
                UPDATE
                    event_property
                SET
                    name = {new_name}
                WHERE 
                    id = {event_property[0]}
            """)


class CleanActions(Clean):
    def __init__(self, event_name, property_name, public_key):
        super().__init__(event_name, property_name, public_key)

    def update_properties_name(self):
        super().change_properties_name(new_name=" ")

    def validate_email(self):
        super().validate_email()

    def cast_integer(self):
        super().cast_integer()

    def remove_white_space(self):
        query = super().get_clean_data_query(old_value=" ", new_value="")
        self.db_instance.handler(query=query)

    def replace_comma_doc(self):
        query = super().get_clean_data_query(old_value=",", new_value=".")
        self.db_instance.handler(query=query)

    def remove_dollar_symbol(self):
        query = super().get_clean_data_query(old_value="$", new_value="")
        self.db_instance.handler(query=query)

    def remove_euro_sign(self):
        query = super().get_clean_data_query(old_value="€", new_value="")
        self.db_instance.handler(query=query)


class CleanString(CleanActions):
    def __init__(self, event_name, property_name, public_key):
        super().__init__(event_name, property_name, public_key)


class CleanFloat(CleanActions):
    def __init__(self, event_name, property_name, public_key):
        super().__init__(event_name, property_name, public_key)


class CleanDate(CleanActions):
    def __init__(self, event_name, property_name, public_key):
        super().__init__(event_name, property_name, public_key)


def main():
    clean_string = CleanString(event_name="SGC_SPEC", property_name="CANTIDAD_CORTESIA", public_key="kKS0DfTKpE8TqUZs")
    print(clean_string.remove_white_space())


if __name__ == '__main__':
    main()
