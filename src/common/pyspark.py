from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.errors import PySparkException


class MySpark:
    """
    Initialize the spark
    """
    @classmethod
    def initialize_spark(cls, mongo_uri: str, name: str = 'Default Project', is_azure: bool = False) -> SparkSession | None:
        """
        initialize spark
        :param is_azure:
        :param mongo_uri: str, mongo connection uri
        :param name: str, spark application name
        :return:
        """
        try:
            # getting the spark instance
            if is_azure:
                spark = SparkSession.builder \
                    .appName(name) \
                    .config("spark.mongodb.input.uri", mongo_uri) \
                    .config("spark.mongodb.output.uri", mongo_uri) \
                    .config("spark.jars.packages", "com.microsoft.azure:spark-mssql-connector_2.12:1.2.0") \
                    .getOrCreate()
            else:
                spark = SparkSession.builder \
                    .appName(name) \
                    .config("spark.mongodb.input.uri", mongo_uri) \
                    .config("spark.mongodb.output.uri", mongo_uri) \
                    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
                    .getOrCreate()
        except PySparkException as e:
            print(f"Failed to get or create Spark: {e}")

            return None
        else:
            return spark
