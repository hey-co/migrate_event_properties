from cryptography.fernet import Fernet
import psycopg2
import os
import logging


class DBInstance:
    def __init__(self, public_key):
        self.public_key = public_key

    def handler(self, query):
        data_conn = self.get_conn_data()
        conn = self.make_conn(data=data_conn)
        result = self.fetch_data(conn=conn, query=query)
        return result

    def decrypt_fernet(self, token):
        key = os.environ["FERNET_KEY"]
        return Fernet(key).decrypt(token.encode()).decode()

    def get_tenants(self):
        tenants_query = "SELECT t.hey_key, t.db_name, t.db_user, t.db_host, t.db_password, t.db_port, t.db_host_for_reading FROM tenant as t;"

        tanants_conn_data = {
            "db_name": os.environ["TENANT_NAME_DB"],
            "db_user": os.environ["TENANT_USER_DB"],
            "db_host": os.environ["HOST_REPLICA"],
            "db_password": os.environ["TENANT_PASSWORD_DB"],
        }

        conn = self.make_conn(data=tanants_conn_data)
        tenants = self.fetch_data(conn=conn, query=tenants_query)

        conn.close()
        return tenants

    def get_conn_data(self):
        for tenant in self.get_tenants():
            if tenant[0]:
                if self.public_key == self.decrypt_fernet(tenant[0]):
                    conn_data = {
                        "db_name": tenant[1],
                        "db_user": tenant[2],
                        "db_host": tenant[6],
                        "db_password": tenant[4],
                    }
        return conn_data

    def make_conn(self, data):
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

    def fetch_data(self, conn, query):
        cursor = conn.cursor()
        cursor.execute(query)
        result = [line for line in cursor.fetchall()]
        cursor.close()
        return result
