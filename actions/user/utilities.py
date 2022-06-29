from data_base import main_db


class Property:
    def __init__(self, user_id: int, property_name: str, public_key: str):
        self.user_id = user_id
        self.property_name = property_name
        self.property = self.__get_user_property()
        self.columns = self.__get_columns()
        self.db_instance = main_db.DBInstance(public_key=public_key)

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

    def __get_user_record(self, column):
        user_record = self.db_instance.handler(
            query=f"""
            SELECT 
                {column}
            FROM 
                user_company 
            WHERE 
                user_id = {self.user_id}
            ;
        """
        )

        return user_record

    def handler(self):

        if len(self.property) > 1:
            pass
        else:
            if self.property[2] == self.__get_user_record(column=self.property[1]):
                pass
            else:
                self.update_user_property(
                    column=self.property[1],
                    value=self.property[2]
                )

                self.delete_property(
                    property_id=self.property[0]
                )

            if self.__get_user_record(column=self.property[1]) == "NULL":
                self.update_user_property(
                    column=self.property[1],
                    value=self.property[2]
                )
                self.delete_property(
                    property_id=self.property[0]
                )
            else:
                pass
            pass

    def update_user_property(self, column, value):
        self.db_instance.handler(
            query=f"""
            UPDATE
                user_company
            SET
                {column} = {value}
            WHERE
                id = {self.user_id}
            ;
        """
        )

    def delete_property(self, property_id):
        self.db_instance.handler(
            query=f"""
            DELETE
                FROM 'user_property'
                WHERE id = {property_id}
            ;
        """
        )


if __name__ == "__main__":
    init = Property(
        user_id=14005983, property_name="Naturaleza", public_key="kKS0DfTKpE8TqUZs"
    )
    print(init.handler())
