import time
from contextlib import contextmanager

import psycopg2


class DBPool:
    TRIES = 3
    DELAY = 100

    def __init__(self, host, port, db_name, user, password, pool_size=4, connection_ttl=100):
        self._free_connections = []
        self._open_connection_quantity = 0
        self._host = host
        self._port = port
        self._db_name = db_name
        self._user = user
        self._password = password
        self._pool_size = pool_size
        self._connection_ttl = connection_ttl

    def __del__(self):
        for connection in self._free_connections:
            self._close_connection(connection)

    def _get_connection(self):
        connection = None
        while not connection:
            if self._free_connections:
                connection = self._free_connections.pop()
            elif self._open_connection_quantity < self._pool_size:
                connection = self._create_connection()
                self._open_connection_quantity += 1
            else:
                time.sleep(self.DELAY)
        return connection

    def _create_connection(self):
        connection = psycopg2.connect(database=self._db_name, user=self._user,
                                      password=self._password, host=self._host, port=self._port)

        return {'connection': connection, 'creation_date': time.time()}

    def _utilize_connection(self, connection):
        if time.time() - connection['creation_date'] < self._connection_ttl:
            self._relax_connection(connection)
        else:
            self._close_connection(connection)

    def _relax_connection(self, connection):
        connection['last_update'] = time.time()
        self._free_connections.append(connection)

    def _close_connection(self, connection):
        self._open_connection_quantity -= 1
        connection['connection'].close()

    # only for select
    @contextmanager
    def get_manager(self):
        connection = self._get_connection()
        cursor = connection['connection'].cursor()
        try:
            yield cursor
        except:
            self._close_connection(connection)
            raise

        self._utilize_connection(connection)

    # for insert update delete
    @contextmanager
    def get_transaction(self):
        connection = self._get_connection()
        cursor = connection['connection'].cursor()
        try:
            yield cursor
            connection['connection'].commit()
        except:
            self._close_connection(connection)
            connection['connection'].rollback()
            raise

        self._utilize_connection(connection)


db_credentials = {
    'host': '127.0.0.1',
    'port': '5432',
    'db_name': 'your_db_name',
    'user': 'your_user_name',
    'password': 'your_password'
}
