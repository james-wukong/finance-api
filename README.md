# finance-api used in big data project

This project is initialized for big data program. We are about to process big data with PySpark by loading data from different data sources, such as csv, mongodb, mysql, or postgresql.

As I have never been an individual investor, during this project, only the relevant information that interests me will be fetched from apis.
 
## How to Use

1. copy conf-sample.yaml to conf.yaml
2. change the configuration based on your env
3. make sure you have a database named 'finance_api' (based on your configuration) in both your mysql and postgresql
4. make sure you have a collection named 'finance_api' (based on your configuration) in your mongodb
3. run

```shell
pip3 -r install requirments.txt
# -i for initialize data (only used when running the program for the first time
# -u for update data, fetching daily data and save in database
python3 main.py -i
```

## read data from difference api sources, such as fmp, finviz and finnhub

### APIs

These are some APIS that I use to fetch data for stock analysis

- Finnhub
- FMP
- Finviz
- yfinance

## format data in json or pandas dataframe

Currently, most of the data will be present as json or dataframe after fetching from API

## store data into mongodb or mariadb

According to the requirement, data will be stored in mongodb, mariadb, postgresql, or even all of them.

## for future data analysis

PySpark will be involved to fetch data from the above distributed systems, utilize machine learning to make predictions and deliver them to Azure-SQL.

Finally, PowerBI will be used to fetch data from Azure-SQL and do the visualization