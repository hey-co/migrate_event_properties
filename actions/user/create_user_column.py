from data_base import main_db


class Column:
    def __init__(self):
        self.db_instance = main_db.DBInstance(public_key="kKS0DfTKpE8TqUZs")

    def handler(self):
        for schema_property in self.get_schema_properties():
            self.add_user_column(schema_property=schema_property)

    def get_schema_properties(self):
        try:
            user_schema_properties = self.db_instance.handler(
                query=f"select id, name, type from property_user_schema where db_status = 'pending_create';"
            )
        except Exception as e:
            raise e
        else:
            return user_schema_properties

    def add_user_column(self, schema_property):
        try:
            self.db_instance.handler(
                query=f"ALTER TABLE user_company ADD {schema_property[1]} {schema_property[2]};"
            )
        except Exception as e:
            raise e
        else:
            self.update_schema_db_status()

    def update_schema_db_status(self, schema_id):
        try:
            self.db_instance.handler(
                query=f"""
                    ALTER TABLE
                        property_user_schema
                    SET 
                        db_status='create_completed', migrate_status='migrate_pending'
                    WHERE id = {schema_id};
                """
            )
        except Exception as e:
            raise e