from typing import List, Tuple, Any
from data_base import main_db
import os


class Users:
    def __init__(self):
        self.db_instance = main_db.DBInstance(public_key=os.environ["DEV_BACK"])

    def handler(self):
        for user in self.get_users_to_migrate():
            self.migrate_user(user=user)
            self.update_migrated_user(user_id=user[0])

    def get_users_to_migrate(self) -> List[Tuple[Any]]:
        try:
            users_to_migrate = self.db_instance.handler(
                query="SELECT * from user_company WHERE email OR mobile_number OR identification;"
            )
        except Exception as e:
            raise e
        else:
            return users_to_migrate

    def migrate_user(self, user):
        try:
            migrate_user = self.db_instance.handler(
                query=f"""INSERT INTO user_identificated(id, email, first_name, last_name, mobile_number, birth_data,
                    updated_at, created_at, anonymous_id, user_uuid, city, country, department, gender, user_ref_id,
                    identification, identification_type) VALUES ({user[0]}, {user[1]}, {user[2]}, {user[3]}, {user[4]}, 
                        {user[5]}, {user[6]}, {user[7]}, {user[8]}, {user[9]}, {user[10]}, {user[11]}, {user[12]}. 
                        {user[13]}, {user[14]}, {user[15]}, {user[16]});"""
            )
        except Exception as e:
            raise e
        else:
            return migrate_user

    def update_migrated_user(self, user_id: int) -> int:
        try:
            update_user = self.db_instance.handler(
                query=f"UPDATE user_company SET migrate ='migrated', WHERE id={user_id};"
            )
        except Exception as e:
            raise e
        else:
            return update_user[0][0]
