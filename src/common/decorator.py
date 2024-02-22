import functools
import json
import os

import mysql.connector
import pandas as pd
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from requests import Response

from src.common.api_exception import ApiException
from src.common.database import MariadbConn, MongoConn
from src.common.pyspark import MySpark


class ApiDecorator:

    @classmethod
    def __prepare_dataframe(cls, spark, sc, data) -> DataFrame:
        """
        prepare dataframe when the data might be in different data types
        :param spark: spark session
        :param sc: spark context
        :param data: data to be processed
        :return: spark sql dataframe
        """
        if isinstance(data, pd.DataFrame) and not data.empty:
            df = spark.createDataFrame(data)
        elif isinstance(data, list):
            df = spark.read.json(sc.parallelize([json.dumps(record) for record in data]))
        elif isinstance(data, dict):
            data = [data]
            df = spark.read.json(sc.parallelize([json.dumps(record) for record in data]))
        else:
            # initialize empty dataframe
            schema = StructType([])
            df = spark.createDataFrame([], schema)

        return df

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
                cnx = MariadbConn.initialize_mariadb_conn(self.maria_conf)
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

    @classmethod
    def write_to_mongodb_sp(cls, collection, database='finance_api'):
        """
        decorator that writes result data into mongodb
        :param database: str, database name
        :param collection: str, collection name
        :return:
        """

        def wrapper(func):
            @functools.wraps(func)
            def _call_wrapper(self, *args, **kwargs):
                response = func(self, *args, **kwargs)
                if isinstance(response, Response):
                    response = response.json()
                if self.write_to_mongo:
                    spark = MySpark.initialize_spark(mongo_uri=self.mongo_uri)
                    sc = spark.sparkContext
                    df = ApiDecorator.__prepare_dataframe(spark, sc, response)
                    config = {
                        'uri': self.mongo_uri,
                        'database': database,
                        'collection': collection
                    }
                    if not df.isEmpty():
                        df.write.format("mongo") \
                            .options(**config) \
                            .mode("append") \
                            .save()
                        spark.stop()
                return response

            return _call_wrapper

        return wrapper


    @classmethod
    def write_to_maria_sp(cls, write_table: str = ''):
        """
        decorator that write the result data into mariadb
        :param write_table: str, table name to write data
        :return:
        """
        def wrapper(func):
            @functools.wraps(func)
            def _call_wrapper(self, *args, **kwargs):
                response = func(self, *args, **kwargs)
                if isinstance(response, Response):
                    response = response.json()
                if self.write_to_mysql:
                    spark = MySpark.initialize_spark(mongo_uri=self.mongo_uri)
                    sc = spark.sparkContext
                    df = ApiDecorator.__prepare_dataframe(spark, sc, response)
                    properties = {key: self.maria_conf[key] for key in self.maria_conf.keys()
                                  & {'user', 'password', 'driver'}}

                    if not df.isEmpty():
                        df.write.jdbc(
                            url=self.maria_jdbc,
                            table=write_table,
                            mode="append",
                            properties=properties
                        )
                    spark.stop()
                return response
            return _call_wrapper
        return wrapper

    @classmethod
    def write_to_postgres_sp(cls, write_table: str = ''):
        """
        decorator that write the result data into mariadb
        :param write_table: str, table name to write data
        :return:
        """
        def wrapper(func):
            @functools.wraps(func)
            def _call_wrapper(self, *args, **kwargs):
                response = func(self, *args, **kwargs)
                if isinstance(response, Response):
                    response = response.json()
                if self.write_to_postgres:
                    spark = MySpark.initialize_spark(mongo_uri=self.mongo_uri)
                    sc = spark.sparkContext
                    df = ApiDecorator.__prepare_dataframe(spark, sc, response)
                    properties = {key: self.postgres_conf[key] for key in self.postgres_conf.keys()
                                  & {'user', 'password', 'driver'}}

                    if not df.isEmpty():
                        df.write.jdbc(
                            url=self.postgres_jdbc,
                            table=write_table,
                            mode="append",
                            properties=properties
                        )
                    spark.stop()
                return response
            return _call_wrapper
        return wrapper

    @classmethod
    def write_to_azure_sp(cls, write_table: str = ''):
        """
        decorator that write the result data into mariadb
        :param write_table: str, table name to write data
        :return:
        """
        def wrapper(func):
            @functools.wraps(func)
            def _call_wrapper(self, *args, **kwargs):
                response = func(self, *args, **kwargs)
                if isinstance(response, Response):
                    response = response.json()
                if self.write_to_mysql:
                    spark = MySpark.initialize_spark(mongo_uri=self.mongo_uri)
                    sc = spark.sparkContext
                    df = ApiDecorator.__prepare_dataframe(spark, sc, response)
                    properties = {key: self.azure_conf[key] for key in self.azure_conf.keys()
                                  & {'user', 'password', 'driver'}}

                    if not df.isEmpty():
                        df.write.jdbc(
                            url=self.azure_jdbc,
                            table=write_table,
                            mode="append",
                            properties=properties
                        )
                    spark.stop()
                return response
            return _call_wrapper
        return wrapper

    @classmethod
    def write_to_hadoop_csv(cls, file_name: str = ''):
        """
        save data in hadoop /user/input/project directory
        :param file_name: str, file name
        :return:
        """
        def wrapper(func):
            @functools.wraps(func)
            def _call_wrapper(self, *args, **kwargs):
                response = func(self, *args, **kwargs)
                if isinstance(response, Response):
                    response = response.json()
                if self.write_to_mysql and response:
                    spark = MySpark.initialize_spark(mongo_uri=self.mongo_uri)
                    sc = spark.sparkContext
                    df = ApiDecorator.__prepare_dataframe(spark, sc, response)
                    df.coalesce(1).write.mode('overwrite') \
                        .option('header', 'true') \
                        .csv(os.path.join(self.hadoop_uri, file_name))
                    spark.stop()
                return response
            return _call_wrapper
        return wrapper
