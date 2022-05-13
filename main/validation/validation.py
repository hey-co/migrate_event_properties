from typing import List, Tuple, Any
from data_base import main_db
from main.base import base

import os


class Validation(base.Base):
    def __init__(self) -> None:
        self.db_instance = main_db.DBInstance(public_key=os.environ["DEV_BACK"])

    def handler(self):
        """
        for i in cleaned_names:
            if i in [j[0] for j in generic_properties]:
                print("Made crosstab")
            else:
                print("Raise exception")
        """

        """
        self.execute_cast(
            value=event_properties[0][0], to_cast=event_schema_property[2]
        )
        """

    def execute_cast(self, value: Any, to_cast: Any):
        try:
            cast = self.db_instance.handler(query=f"SELECT {value}::{to_cast};")
        except Exception as e:
            raise e
        else:
            return cast


if __name__ == "__main__":
    validation = Validation()
    validation.handler()
