from data_base import main_db


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


class CleanActions(Clean):
    def __init__(self, event_name, property_name, public_key):
        super().__init__(event_name, property_name, public_key)

    def remove_white_space(self):
        query = super().get_clean_data_query(old_value=" ", new_value="")
        super().db_instance.handler(query=query)

    def replace_comma_doc(self):
        query = super().get_clean_data_query(old_value=",", new_value=".")
        super().db_instance.handler(query=query)

    def remove_dollar_symbol(self):
        query = super().get_clean_data_query(old_value="$", new_value="")
        super().db_instance.handler(query=query)

    def remove_euro_sign(self):
        query = super().get_clean_data_query(old_value="€", new_value="")
        super().db_instance.handler(query=query)


class CleanString(CleanActions):
    def __init__(self, event_name, property_name, public_key):
        super().__init__(event_name, property_name, public_key)

    super().remove_white_space()
    super().replace_comma_doc()


class CleanFloat(CleanActions):
    def __init__(self, event_name, property_name, public_key):
        super().__init__(event_name, property_name, public_key)

    super().replace_comma_doc()
    super().remove_dollar_symbol()
    super().remove_euro_sign()


class CleanDate(CleanActions):
    def __init__(self, event_name, property_name, public_key):
        super().__init__(event_name, property_name, public_key)

    super().replace_comma_doc()


def main():
    clean_string = CleanString(event_name="SGC_SPEC", property_name="CANTIDAD_CORTESIA", public_key="kKS0DfTKpE8TqUZs")
    print(clean_string.remove_white_space())


if __name__ == '__main__':
    main()
