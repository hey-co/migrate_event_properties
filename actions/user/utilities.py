from data_base import main_db


class Property:
    def __init__(self, user_id: int, property_name: str, public_key: str):
        self.db_instance = main_db.DBInstance(public_key=public_key)
        self.user_id = user_id
        self.property_name = property_name
        self.property = self.__get_user_property()
        self.columns = self.__get_columns()

    def __get_columns(self):
        columns = self.db_instance.handler(
            query=f"""
            SELECT
                column_name
            FROM
                INFORMATION_SCHEMA.COLUMNS
            WHERE
                TABLE_NAME = 'user_company'
            ;
        """
        )
        return columns

    def __get_user_property(self):
        properties = self.db_instance.handler(
            query=f"""
            SELECT 
                *
            FROM 
                user_property 
            WHERE 
                user_id = {self.user_id}
            AND 
                name LIKE '{self.property_name}'
            ;
            """
        )
        return properties

    def get_user_record(self, column):
        user_record = self.db_instance.handler(
            query=f"""
                SELECT 
                    {column}
                FROM 
                    user_company 
                WHERE 
                    id = {self.user_id}
                ;
        """
        )
        return user_record[0][0]

    def migrate_property(self):
        if self.property[0][1] in [c[0] for c in self.columns]:
            if self.property[0][2] == self.get_user_record(column=self.property[0][1]):
                self.delete_property(
                    property_id=self.property[0][0]
                )
            else:
                self.update_user_property(
                    column=self.property[0][1],
                    value=self.property[0][2]
                )

                self.delete_property(
                    property_id=self.property[0][0]
                )

            if self.get_user_record(column=self.property[0][1]) is None:
                self.update_user_property(
                    column=self.property[0][1],
                    value=self.property[0][2]
                )

                self.delete_property(
                    property_id=self.property[0][0]
                )
            else:
                pass
            pass
        else:
            pass

    def handler(self):
        if self.property:
            if len(self.property) > 1:
                if len(set([p[2] for p in self.property if p[2]])) > 1:
                    pass
                else:
                    self.migrate_property()
            else:
                if self.property[0][2]:
                    self.migrate_property()
                else:
                    self.delete_property(
                        property_id=self.property[0][0]
                    )
        else:
            pass

    def update_user_property(self, column, value):
        if type(value) == str:
            query = f"""
                UPDATE
                    user_company
                SET
                    {column} = '{value}'
                WHERE
                    id = {self.user_id}
                ;
            """
        else:
            query = f"""
                UPDATE
                    user_company
                SET
                    {column} = {value}
                WHERE
                    id = {self.user_id}
                ;
            """

        self.db_instance.handler(
            query=query
        )

    def delete_property(self, property_id):
        self.db_instance.handler(
            query=f"""
                DELETE FROM user_property
                WHERE id = {property_id}
            ;
        """
        )


if __name__ == "__main__":
    basic_properties = [
        "email"
    ]

    for basic_property in basic_properties:
        conn = main_db.DBInstance(public_key="kKS0DfTKpE8TqUZs")
        user_ids = conn.handler(query=f"select distinct user_id from user_property where name like '{basic_property}';")
        if user_ids:
            for user_id in user_ids:
                init = Property(
                    user_id=user_id[0], property_name=basic_property, public_key="kKS0DfTKpE8TqUZs"
                )
                init.handler()
