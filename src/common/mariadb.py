from __future__ import annotations

import mysql.connector
from mysql.connector import errorcode, MySQLConnection
from mysql.connector.pooling import PooledMySQLConnection


class MariadbConn:
    """
    Initialize mariaDB connection
    """
    @classmethod
    def initialize_mariadb_conn(cls, conf=None) -> MySQLConnection | PooledMySQLConnection | None:
        try:
            cnx = mysql.connector.connect(**conf)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
            return None
        else:
            return cnx
