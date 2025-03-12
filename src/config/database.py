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
    """Database is a singleton class that provides a single instance of the database connection pool."""

    _instance = None
    _connection_pool = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Database, cls).__new__(cls)
            cls._instance.init_pool()
        return cls._instance

    def init_pool(self):
        """Initializes the connection pool."""
        self._connection_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=5,
            maxconn=50,
            host=DATABASE_HOST,
            database=DATABASE_NAME,
            user=DATABASE_USER,
            password=DATABASE_PASSWORD,
            port=DATABASE_PORT,
        )

    def get_connection(self):
        """Returns a connection from the pool."""
        return self._connection_pool.getconn()

    def release_connection(self, conn):
        """Releases the connection back to the pool."""
        self._connection_pool.putconn(conn)

    def close_pool(self):
        """Closes all connections in the pool."""
        self._connection_pool.closeall()

    def health_check(self):
        """Performs a health check on the database."""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.close()
            self.release_connection(conn)
            return True
        except Exception as e:
            print(f"Health check failed: {e}")
            return False
