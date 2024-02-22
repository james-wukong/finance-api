import yaml

from src.finnhub import FinnhubApi
from src.fmp import FmpApi

if __name__ == '__main__':
    with open('conf.yaml', 'r') as file:
        config = yaml.safe_load(file)

    # initialize FinnhubApi
    fin_api = FinnhubApi(base_url=config['api']['finnhub']['api_endpoint'],
                         api_key=config['api']['finnhub']['token'],
                         write_to_mongo=True,
                         write_to_mysql=True,
                         write_to_postgres=True,
                         maria_conf=config['mariadb'],
                         hadoop_conf=config['hadoop'],
                         postgres_conf=config['postgres'],
                         mongo_conf=config['mongodb'], )

    news_params = {'symbol': 'AAPL', 'from': '2023-08-15', 'to': '2024-01-20'}
    # get company news and save in mongodb
    news = fin_api.fetch_company_news(params=news_params)

    insider_params = {
        'symbol': 'AAPL',
        'from': '2023-08-15',
        'to': '2024-01-20'
    }
    # get insider transactions and save in mongodb
    insider = fin_api.fetch_insider_transactions(params=insider_params)

    # initialize FmpApi
    fmp_api = FmpApi(base_url=config['api']['fmp']['api_endpoint'],
                     api_key=config['api']['fmp']['token'],
                     write_to_mongo=True,
                     write_to_mysql=True,
                     write_to_postgres=True,
                     maria_conf=config['mariadb'],
                     hadoop_conf=config['hadoop'],
                     postgres_conf=config['postgres'],
                     mongo_conf=config['mongodb'],
                     )
    fmp_ticker_params = {'query': 'AA', 'exchange': 'NASDAQ'}
    # get company ticker and save in mariadb
    companies = fmp_api.fetch_company_ticker(params=fmp_ticker_params)
    category_profile = 'TSLA'
    company = fmp_api.fetch_company_profile(category=category_profile)
    category_chart = 'AAPL'
    chart = fmp_api.fetch_daily_chart(category=category_chart)
    category_rating = 'TSLA'
    ratings = fmp_api.fetch_historical_rating(category_rating)
