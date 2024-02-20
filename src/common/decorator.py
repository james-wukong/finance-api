import functools

import mysql.connector
import pandas as pd

from src.common.api_exception import ApiException
from src.common.mongodb import MongoConn
from src.common.mariadb import MariadbConn


class ApiDecorator:

    @classmethod
    def inject_api_key(cls, param_api: str):
        """
        decorator that injects apikey to request uri
        :param param_api: str, different api might implement different name in their uri, such as, 'token', 'apikey'
        :return:
        """
        def wrapper(func):

            @functools.wraps(func)
            def _call_wrapper(self, *args, **kwargs):
                api_key = self.api_key
                request = func(self, *args, **kwargs)
                if '?' not in request:
                    return request + f"?{param_api}=" + api_key
                else:
                    return request + f"&{param_api}=" + api_key

            return _call_wrapper

        return wrapper

    @classmethod
    def format_data(cls, func):
        """
        decorator that formats the response data
        :param func:
        :return:
        """
        @functools.wraps(func)
        def _call_wrapper(self, *args, **kwargs):
            response = func(self, *args, **kwargs)
            if self.output_format == 'json':
                return response.json()
            elif self.output_format == 'pandas':
                return pd.DataFrame(response.json())
            else:
                raise ApiException("Output must be either pandas or json",
                                   ApiDecorator.format_data.__name__)

        return _call_wrapper

    @classmethod
    def write_to_mongodb(cls, db, col):
        """
        decorator that writes result data into mongodb
        :param db: str, database name
        :param col: str, collection name
        :return:
        """
        def wrapper(func):

            @functools.wraps(func)
            def _call_wrapper(self, *args, **kwargs):
                response = func(self, *args, **kwargs)
                if self.write_to_mongo and response:
                    mongo_conn = MongoConn.initialize_mongodb_client(
                        self.mongo_uri)
                    collection = mongo_conn[db][col]
                    collection.insert_many(response)
                return response

            return _call_wrapper

        return wrapper

    @classmethod
    def write_to_mariadb(cls, func):
        """
        decorator that write the result data into mariadb
        :param func:
        :return:
        """
        @functools.wraps(func)
        def _call_wrapper(self, *args, **kwargs):
            response, stmt = func(self, *args, **kwargs)
            if self.write_to_mysql and response:
                cnx = MariadbConn.initialize_mariadb_conn(self.mariadb_conf)
                cursor = cnx.cursor()
                try:
                    cursor.executemany(stmt, response)
                    cnx.commit()
                except mysql.connector.Error as err:
                    print(cursor.statement)
                    print("Error Happened Here?", err)
                    cnx.rollback()
                finally:
                    if cnx.is_connected():
                        cursor.close()
                        cnx.close()

        return _call_wrapper
