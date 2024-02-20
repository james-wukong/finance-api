import mysql.connector
from mysql.connector import errorcode


class MariadbConn:
    """
    Initialize mariaDB connection
    """

    @classmethod
    def initialize_mariadb_conn(cls, conf=None):
        try:
            cnx = mysql.connector.connect(**conf)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
        else:
            return cnx
