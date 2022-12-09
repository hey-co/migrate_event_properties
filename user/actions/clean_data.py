from data_base import main_db
from typing import List, Tuple, Any, Dict
import re


class Clean:
    def __init__(self, property_name: str, public_key: str):
        self.property_name = property_name
        self.properties = self.__get_user_properties()
        self.db_instance = main_db.DBInstance(public_key=public_key)

    def get_clean_data_query(self, old_value: str, new_value: str) -> str:
        query: str = f"""
            UPDATE
                user_property
            SET
                value = regexp_replace(value, '{old_value}', '{new_value}')
            WHERE
                user_id in (
                    select id 
                    from user_company 
                    where 
                        NOT EXISTS email or 
                        NOT EXISTS mobile_number or
                        NOT EXISTS identification
                    )
                AND name LIKE '{self.property_name}'
            ;
            """
        return query

    def __get_user_properties(self) -> List[Tuple[Any]]:
        properties = self.db_instance.handler(query=f"""
            SELECT 
                id, 
                value 
            FROM 
                user_property 
            WHERE 
                user_id in (
                    select id 
                    from user_company 
                    where 
                        email IS NULL or 
                        mobile_number IS NULL or
                        identification IS NULL
                )
            AND 
                name LIKE '{self.property_name}';
            """)
        return properties

    def cast_integer(self) -> None:
        for user_property in self.properties:
            self.db_instance.handler(query=f"""
                UPDATE
                    user_property            
                SET
                    value = CAST(({user_property[2]}) AS int)
                WHERE
                    id = {user_property[0]}
            """)

    def validate_email(self) -> Dict[str, bool]:
        result = {}
        for user_property in self.properties:
            if re.match(r"[^@]+@[^@]+\.[^@]+", user_property[2]):
                result[user_property[2]] = True
            else:
                result[user_property[2]] = False
        return result

    def change_properties_name(self, new_name: str) -> None:
        for user_property in self.properties:
            self.db_instance.handler(query=f"""
                UPDATE
                    user_property
                SET
                    name = {new_name}
                WHERE 
                    id = {user_property[0]}
            """)

    def delete_value(self) -> None:
        for ep in self.properties:
            self.db_instance.handler(query=f"""
                UPDATE
                    user_property
                SET
                    value = NULL
                WHERE id = {ep[0]}
            """)


class CleanActions(Clean):
    def __init__(self, property_name, public_key):
        super().__init__(property_name, public_key)

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

    def update_properties_name(self):
        super().change_properties_name(new_name="")

    def delete_value(self) -> None:
        super().delete_value()

    def replace_text(self) -> None:
        query = super().get_clean_data_query(old_value="New text", new_value="")
        self.db_instance.handler(query=query)


class CleanString(CleanActions):
    def __init__(self, property_name, public_key):
        super().__init__(property_name, public_key)


class CleanFloat(CleanActions):
    def __init__(self, property_name, public_key):
        super().__init__(property_name, public_key)


class CleanDate(CleanActions):
    def __init__(self, property_name, public_key):
        super().__init__(property_name, public_key)


def main():
    clean_string = CleanString(property_name="CANTIDAD_CORTESIA", public_key="kKS0DfTKpE8TqUZs")
    print(clean_string.remove_white_space())


if __name__ == '__main__':
    main()
