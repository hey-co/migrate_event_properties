from data_base import main_db


class UserSchemaProperties:
    def __init__(self):
        self.db_instance = main_db.DBInstance(public_key="**")

    def get_schema_properties(self):
        try:
            user_schema_properties = self.db_instance.handler(
                query=f"""
                    SELECT 
                        * 
                    FROM 
                        property_user_schema 
                    WHERE 
                        db_status='create_completed' AND migrate_status='migrate_pending';
                """
            )
        except Exception as e:
            raise e
        else:
            return user_schema_properties

    def get_user_property_by_schema_property_name(self, schema_property_name):
        try:
            user_properties = self.db_instance.handler(
                query=f"select * from user_property where name='{schema_property_name}';"
            )
        except Exception as e:
            raise e
        else:
            return user_properties

    def update_migrate_status(self, schema_property_id, status):
        try:
            self.db_instance.handler(
                query=f"""
                    UPDATE 
                        property_user_schema
                    SET 
                        migrate_status={status}
                    WHERE 
                        id={schema_property_id};
                """
            )
        except Exception as e:
            raise e

    def update_user_value(self, user_property):
        try:
            self.db_instance.handler(
                query=f"""
                    UPDATE 
                        user_company
                    SET
                        {user_property[1]} = {user_property[2]}
                    WHERE
                        id = {user_property[3]};
                """
            )
        except Exception as e:
            raise e
        else:
            self.__delete_user_property(property_id=user_property[0])

    def __delete_user_property(self, property_id):
        try:
            self.db_instance.handler(
                query=f"""
                    DELETE FROM
                        user_property
                    WHERE
                        id = {property_id};
                """
            )
        except Exception as e:
            raise e

    def handler(self):
        for schema_property in self.get_schema_properties():
            self.update_migrate_status(schema_property_id=schema_property[0], status="migrate_in_progress")
            for user_property in self.get_user_property_by_schema_property_name(
                    schema_property_name=schema_property[1]
            ):
                self.update_user_value(property=user_property)
            self.update_migrate_status(
                schema_property_id=schema_property[0],
                status="migrate_completed"
            )
