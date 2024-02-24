from __future__ import annotations

import os
from abc import ABC


class ApiInterface(ABC):
    def __init__(self,
                 base_url=None,
                 api_key=None,
                 output_format='json',
                 write_to_mysql=False,
                 write_to_postgres=False,
                 write_to_azure=False,
                 write_to_mongo=False,
                 mongo_conf: dict = None,
                 hadoop_conf: dict = None,
                 maria_conf: dict = None,
                 azure_conf: dict = None,
                 postgres_conf: dict = None):
        self.base_url = base_url
        self.api_key = api_key
        self.output_format = output_format
        self.write_to_mysql = write_to_mysql
        self.write_to_postgres = write_to_postgres
        self.write_to_mongo = write_to_mongo
        self.write_to_azure = write_to_azure
        self.mongo_conf = mongo_conf
        self.maria_conf = maria_conf
        self.postgres_conf = postgres_conf
        self.azure_conf = azure_conf
        self.hadoop_conf = hadoop_conf
        self.mongo_uri = self.__generate_mongo_uri()
        self.hadoop_uri = self.__generate_hadoop_uri()
        self.maria_jdbc = self.__generate_maria_jdbc()
        self.postgres_jdbc = self.__generate_postgres_jdbc()
        self.azure_jdbc = self.__generate_azure_jdbc()

    def __generate_mongo_uri(self) -> str | None:
        """
        generate mongo connection uri based on input
        :return:
        """
        if not self.mongo_conf:
            return None
        return (f"mongodb+srv://{self.mongo_conf['user']}:"
                f"{self.mongo_conf['token']}@"
                f"{self.mongo_conf['host']}"
                f"/?retryWrites=true&w=majority")

    def __generate_hadoop_uri(self) -> str | None:
        """
        generate mongo connection uri based on input
        :return:
        """
        if not self.mongo_conf:
            return None
        return os.path.join(f"hdfs://{self.hadoop_conf['host']}:{self.hadoop_conf['port']}",
                            self.hadoop_conf['dir'])

    def __generate_maria_jdbc(self) -> str | None:
        if not self.maria_conf:
            return None
        return (f"jdbc:mysql://{self.maria_conf['host']}:"
                f"{self.maria_conf['port']}/"
                f"{self.maria_conf['database']}?permitMysqlScheme")

    def __generate_postgres_jdbc(self) -> str | None:
        if not self.postgres_conf:
            return None
        return (f"jdbc:postgresql://{self.postgres_conf['host']}:"
                f"{self.postgres_conf['port']}/"
                f"{self.postgres_conf['database']}")

    def __generate_azure_jdbc(self) -> str | None:
        if not self.azure_conf:
            return None
        return (f"jdbc:sqlserver://{self.azure_conf['host']}:{self.azure_conf['port']};"
                f"databaseName={self.azure_conf['database']};"
                f"user={self.azure_conf['user']}@{self.azure_conf['db_resource']};"
                f"password={self.azure_conf['password']};encrypt=true;")
