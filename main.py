import argparse
from datetime import date, timedelta

import yaml

from src.finnhub import FinnhubBaseApi
from src.fmp import FmpBaseApi
from src.yfinance import YFBaseApi

if __name__ == '__main__':
    with open('conf.yaml', 'r') as file:
        config = yaml.safe_load(file)

    yesterday = str(date.today() - timedelta(days=1))
    # data history length: years
    end_date, start_date = yesterday, yesterday
    duration, update = 20, True

    parser = argparse.ArgumentParser(
        prog="main.py",
        description="get data from api and store them in database",
        epilog="Thanks for using %(prog)s! :)",
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("-i", "--initialize", action="store_true",
                       help='use this if you want to initialize all data (for the first run)')
    group.add_argument("-u", "--update", action="store_false",
                       help='use this if you want to update data')
    args = parser.parse_args()

    # using command python3 main.py -i or python3 main.py --initialize when run for the first time
    if args.initialize:
        confirm = input('Are you initializing historical data? [Y/N]')
        if confirm.upper() == 'Y':
            # get start date
            x = end_date.split('-')
            x[0] = str(int(x[0]) - duration)
            start_date, update = '-'.join(x), False

    # data requirement configurations
    stock_symbols = [
        'AAPL', 'MSFT', 'NVDA', 'META', 'AMZN', 'TSLA', 'GOOGL',
        'ON', 'DBD', 'DSGX', 'GTLB', 'LOGI', 'CRSR',
        'LNG', 'SWN', 'APA', 'BTU', 'CL', 'CVX', 'XOM',
        'BMY', 'THC', 'TNDM',
        'MOS', 'AXTA', 'KOP',
        'SBLK', 'EME', 'DNOW',
    ]

    # initialize APIs
    finn_api = FinnhubBaseApi(
        base_url=config['api']['finnhub']['api_endpoint'],
        api_key=config['api']['finnhub']['token'],
        write_to_mongo=True,
        write_to_mysql=True,
        write_to_postgres=True,
        maria_conf=config['mariadb'],
        hadoop_conf=config['hadoop'],
        postgres_conf=config['postgres'],
        azure_conf=config['azuresql'],
        mongo_conf=config['mongodb'],
    )

    yf_api = YFBaseApi(
        write_to_mongo=True,
        write_to_mysql=True,
        write_to_postgres=True,
        maria_conf=config['mariadb'],
        hadoop_conf=config['hadoop'],
        postgres_conf=config['postgres'],
        mongo_conf=config['mongodb'],
        azure_conf=config['azuresql'],
    )

    fmp_api = FmpBaseApi(
        base_url=config['api']['fmp']['api_endpoint'],
        api_key=config['api']['fmp']['token'],
        write_to_mongo=True,
        write_to_mysql=True,
        write_to_postgres=True,
        maria_conf=config['mariadb'],
        hadoop_conf=config['hadoop'],
        postgres_conf=config['postgres'],
        azure_conf=config['azuresql'],
        mongo_conf=config['mongodb'],
    )

    for symbol in stock_symbols:
        fmp_params = {
            'query': symbol,
        }
        finn_params = {
            'symbol': symbol,
            'from': start_date,
            'to': end_date,
        }
        yf_params = {
            'symbol': symbol,
            'start': start_date,
            'end': end_date,
        }
        # get historical data from yfinance, and save them in mysql and postgresql OK
        yf_api.fetch_historical_data(**yf_params)

        # get company news from finnhub api, and saved in mysql and postgresql OK
        finn_api.fetch_company_news(params=finn_params)
        # get inside transactions, and saved in mongodb OK
        finn_api.fetch_insider_transactions(params=finn_params)

        # get company related information and stored in mysql and postgresql OK
        fmp_api.fetch_company_ticker(params=fmp_params)

        if not update:
            # get company profile, and save in mongodb and csv files
            fmp_api.fetch_company_profile(symbol=symbol)
        # get historical company rating and stored in mysql and postgresql OK
        fmp_api.fetch_historical_rating(symbol=symbol)
        # get stock news and stored in mysql and postgresql
        # not tested because need have a paid api to fetch data
        # fmp_api.fetch_stock_news(params=fmp_params)
