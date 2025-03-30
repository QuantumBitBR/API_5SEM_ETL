import psycopg2
from psycopg2 import pool

from .enviroments_custom import (
    DATABASE_HOST,
    DATABASE_NAME,
    DATABASE_PASSWORD,
    DATABASE_PORT,
    DATABASE_USER,
)


class Database:
    _instance = None
    _pool = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(Database, cls).__new__(cls, *args, **kwargs)
            cls._pool = pool.ThreadedConnectionPool(
                minconn=5,
                maxconn=50,
                host=DATABASE_HOST,
                database=DATABASE_NAME,
                user=DATABASE_USER,
                password=DATABASE_PASSWORD,
                port=DATABASE_PORT,
            )
        return cls._instance

    def get_connection(self):
        return self._pool.getconn()

    def release_connection(self, conn):
        self._pool.putconn(conn)

    def close_pool(self):
        self._pool.closeall()

    def health_check(self):
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            self.release_connection(conn)
            return True
        except Exception as e:
            return False
