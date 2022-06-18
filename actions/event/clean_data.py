from data_base import main_db


class CleanData:
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


class CleanString(CleanData):
    def __init__(self, event_name, property_name, public_key, old_value, new_value):
        self.old_value = old_value
        self.new_value = new_value
        super().__init__(event_name, property_name, public_key)

    def clean_data(self):
        query = super().get_clean_data_query(old_value=self.old_value, new_value=self.new_value)
        super().db_instance.handler(query=query)


class CleanFloat(CleanData):
    def __init__(self, event_name, property_name, public_key, old_value, new_value):
        self.old_value = old_value
        self.new_value = new_value
        super().__init__(event_name, property_name, public_key)

    def clean_data(self):
        query = super().get_clean_data_query(old_value=self.old_value, new_value=self.new_value)
        super().db_instance.handler(query=query)


class CleanDate(CleanData):
    def __init__(self, event_name, property_name, public_key, old_value, new_value):
        self.old_value = old_value
        self.new_value = new_value
        super().__init__(event_name, property_name, public_key)

    def clean_data(self):
        query = super().get_clean_data_query(old_value=self.old_value, new_value=self.new_value)
        super().db_instance.handler(query=query)
