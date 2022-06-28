from data_base import main_db


class Clean:
    def __init__(self, user_name: str, property_name: str, public_key: str):
        self.user_name = user_name
        self.property_name = property_name
        self.properties = self.__get_user_properties()
        self.db_instance = main_db.DBInstance(public_key=public_key)

    def __get_user_properties(self):
        properties = self.db_instance.handler(query=f"""
            SELECT 
                property_id, value 
            FROM 
                user_property 
            WHERE 
                user_id in (
                    select id from user_event where name = '{self.user_name}' LIMIT 200
                ) 
            AND 
                name LIKE '{self.property_name}';
            """)
        return properties

    def change_properties_name(self, new_name: str):
        for user_property in self.properties:
            self.db_instance.handler(query=f"""
                UPDATE
                    user_property
                SET
                    name = {new_name}
                WHERE 
                    id = {user_property[0]}
            """)
