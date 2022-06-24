from data_base import main_db
from typing import List, Tuple, Any


class Clean:
    def __init__(self, event_name: str, property_name: str, public_key: str):
        self.event_name: str = event_name
        self.property_name: str = property_name
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

    def get_property_values(self) -> List[Tuple[Any]]:
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

    def update_property_value(self, data):
        self.db_instance.handler(query=f"""
            UPDATE
                event_property            
            WHERE
                id = {data[0]}
            SET
                value = CAST(({data[1]}) AS int)
        """)


class CleanActions(Clean):
    def __init__(self, event_name, property_name, public_key):
        super().__init__(event_name, property_name, public_key)

    def get_remove_zeros_query(self):
        for property_value in super().get_property_values():
            super().update_property_value(data=property_value)

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
