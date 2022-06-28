from data_base import main_db


class Property:
    def __init__(self, user_id: str, property_name: str, public_key: str):
        self.user_id = user_id
        self.property_name = property_name
        self.properties = self.__get_user_properties()
        self.db_instance = main_db.DBInstance(public_key=public_key)

    def __get_user_columns(self, user_id):
        columns = self.db_instance.handler(query=f"""
            SELECT
                column_name
            FROM
                INFORMATION_SCHEMA.COLUMNS
            WHERE
                TABLE_NAME = 'user_company' AND id = {user_id}
            ;
        """)
        return columns

    def __get_user_properties(self):
        properties = self.db_instance.handler(query=f"""
            SELECT 
                *
            FROM 
                user_property 
            WHERE 
                user_id in (
                    select id from user_company where name = '{self.user_id}' LIMIT 200
                ) 
            AND 
                name LIKE '{self.property_name}';
            """)
        return properties

    def filter_properties(self):
        return list(filter(lambda x: x[1] in self.__get_user_columns(user_id=self.user_id), self.properties))

    def handler(self):
        for filter_property in self.filter_properties():
            self.update_user_property(column=filter_property[1], value=filter_property[2])
            self.delete_property(property_id=filter_property[0])

    def update_user_property(self, column, value):
        self.db_instance.handler(query=f"""
            UPDATE
                user_company
            SET
                {column} = {value}
            WHERE
                id = {self.user_id}
            ;
        """)

    def delete_property(self, property_id):
        self.db_instance.handler(query=f"""
            DELETE
                FROM 'user_property'
                WHERE id = {property_id}
            ;
        """)
