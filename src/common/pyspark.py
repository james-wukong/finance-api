from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.errors import PySparkException


class MySpark:
    """
    Initialize the spark
    """
    @classmethod
    def initialize_spark(cls, mongo_uri: str, name: str = 'Default Project') -> SparkSession | None:
        """
        initialize spark
        :param mongo_uri: str, mongo connection uri
        :param name: str, spark application name
        :return:
        """
        try:
            # getting the spark instance
            spark = SparkSession.builder \
                .appName(name) \
                .config("spark.mongodb.input.uri", mongo_uri) \
                .config("spark.mongodb.output.uri", mongo_uri) \
                .config("spark.jars.packages", "com.microsoft.azure:spark-mssql-connector_2.12:1.2.0,"
                                               "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
                .getOrCreate()
        except PySparkException as e:
            print(f"Failed to get or create Spark: {e}")

            return None
        else:
            return spark
