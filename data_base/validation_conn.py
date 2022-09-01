from cryptography.fernet import Fernet
import psycopg2
import os
import logging


class DBInstance:
    def __init__(self, public_key):
        self.public_key = public_key

    @staticmethod
    def __decrypt_fernet(token: str):
        key = os.environ["FERNET_KEY"]
        return Fernet(key).decrypt(token.encode()).decode()

    @staticmethod
    def __get_tenants_query():
        return """
            SELECT
                t.hey_key,
                t.db_name, 
                t.db_user, 
                t.db_host, 
                t.db_password
            FROM 
                tenant as t;
        """

    def __get_tenants(self):
        try:
            conn = psycopg2.connect(
                "dbname='%s' user='%s' host='%s' password='%s'"
                % (
                    os.environ["TENANT_NAME_DB"],
                    os.environ["TENANT_USER_DB"],
                    os.environ["TENANT_HOST_DB"],
                    os.environ["TENANT_PASSWORD_DB"]
                )
            )
        except Exception as e:
            raise e

        cursor = conn.cursor()
        cursor.execute(self.__get_tenants_query())

        raw = cursor.fetchall()
        result = [line for line in raw]
        cursor.close()
        return result

    def __get_conn_data(self):
        conn_data = {}
        for tenant in self.__get_tenants():
            if tenant[0]:
                if self.public_key == self.__decrypt_fernet(tenant[0]):
                    conn_data = {
                        "db_name": tenant[1],
                        "db_user": tenant[2],
                        "db_host": "migrate-properties.csry9lg2mjjk.us-east-1.rds.amazonaws.com",
                        "db_password": "qwerty123.",
                    }
        return conn_data

    def make_conn(self):
        data = self.__get_conn_data()
        try:
            conn = psycopg2.connect(
                "dbname='%s' user='%s' host='%s' password='%s'"
                % (
                    data["db_name"],
                    data["db_user"],
                    data["db_host"],
                    data["db_password"],
                )
            )
        except (Exception, psycopg2.DatabaseError) as error:
            logging.exception(str(error))
        else:
            return conn
