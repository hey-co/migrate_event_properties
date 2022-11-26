

class UserSchemaProperties:
    def __init__(self):
        pass

    def get_schema_properties(self):
        try:
            user_schema_properties = self.db_instance.handler(
                query=f"select * from property_user_schema where migrate_status = 'MIGRATE_PENDING' limit 1;"
            )
        except Exception as e:
            raise e
        else:
            return user_schema_properties

    def update_schema_property_migrate_status(self, property_id: str):
        try:
            self.db_instance.handler(
                query=f"update property_user_schema where id = {property_id} set migrate_status = 'MIGRATE_IN_PROGRESS';"
            )
        except Exception as e:
            raise e

    def get_user_property_by_schema_property_name(self, schema_property_name):
        try:
            user_properties = self.db_instance.handler(
                query=f"select * from user_property where name='{schema_property_name} limit 100';"
            )
        except Exception as e:
            raise e
        else:
            return user_properties

    def update_user_value(self, property_data):
        pass

    def handler(self):
        for schema_property in self.get_schema_properties():
            self.update_schema_property_migrate_status(property_id=schema_property[0])
            for user_property in self.get_user_property_by_schema_property_name(schema_property_name=schema_property[1]):
                self.update_user_value(
                    property_data={"name": user_property[1], "value": user_property[1]}
                )

