from contextlib import contextmanager

import psycopg2
import time


class CustomConnection:
    def __init__(self, ttl, database, user, password, host, port):
        self.ttl = ttl
        self.creation_date = time.time()
        self.is_closed = False
        self.connection = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        
    def close(self):
        self.is_closed = True
        self.connection.close()


class DBPool:
    def __init__(self, db_name, username, password, host, port, max_active_connections=4, connection_ttl=2):
        self._free_connections_pool = []
        self._max_active_connections = max_active_connections
        self._active_connections_quantity = 0
        self._connection_ttl = connection_ttl
        self._db_credentials = {'database': db_name, 'user': username, 'password': password, 'host': host, 'port': port}

    def _create_connection(self):
        try:
            connection = CustomConnection(self._connection_ttl, **self._db_credentials)
            self._active_connections_quantity += 1
            return connection
        except:
            raise Exception('Cannot create connection')

    def _get_connection(self):
        connection = None
        while not connection:
            if self._free_connections_pool:
                connection = self._free_connections_pool.pop()
                if connection.is_closed:
                    connection = None
            elif self._active_connections_quantity < self._max_active_connections:
                connection = self._create_connection()
            else:
                time.sleep(0.5)
        return connection

    def _manage_connection(self, connection):
        print('manager')
        if time.time() < connection.creation_date + connection.ttl:
            self._free_connections_pool.append(connection)
        else:
            connection.close()

    @contextmanager
    def get_connection(self):
        connection = self._get_connection()
        yield connection
        self._manage_connection(connection)
