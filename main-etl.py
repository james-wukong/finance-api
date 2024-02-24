import yaml

from src.common.baseapi import BaseApi
from src.common.pyspark import MySpark

if __name__ == '__main__':
    with open('conf.yaml', 'r') as file:
        config = yaml.safe_load(file)

    settings = BaseApi(
        base_url=config['api']['finnhub']['api_endpoint'],
        api_key=config['api']['finnhub']['token'],
        maria_conf=config['mariadb'],
        hadoop_conf=config['hadoop'],
        postgres_conf=config['postgres'],
        azure_conf=config['azuresql'],
        mongo_conf=config['mongodb'],
    )

    spark = MySpark.initialize_spark(mongo_uri=settings.mongo_uri, name='Big Data Project ETL')

    # read from mysql
    mysql_df = spark.read.jdbc(
        url=settings.maria_jdbc,
        table='(select * from finn_company_news limit 10) t',
        properties={
            'user': config['mariadb']['user'],
            'password': config['mariadb']['password'],
            'driver': config['mariadb']['driver'],
        }
    )

    print(mysql_df.show())

    mongo_df = spark.read.format('mongo') \
        .option("database", 'finance_api') \
        .option("collection", 'finn_insider_transactions') \
        .load()

    print(mongo_df.show())
