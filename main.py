import yaml

from src.finnhub import FinnhubApi
from src.fmp import FmpApi
from src.yfinance import YFApi

if __name__ == '__main__':
    with open('conf.yaml', 'r') as file:
        config = yaml.safe_load(file)

    # data requirement configurations
    stock_symbols = [
        'AAPL', 'MSFT', 'NVDA', 'META', 'AMZN', 'TSLA', 'GOOGL',
        'ON', 'DBD', 'DSGX', 'GTLB', 'LOGI', 'CRSR',
        'LNG', 'SWN', 'APA', 'BTU', 'CL', 'CVX', 'XOM',
        'BMY', 'THC', 'TNDM',
        'MOS', 'AXTA', 'KOP',
        'SBLK', 'EME', 'DNOW',
    ]
    # data history length: years
    end_date = '2022-01-01'
    duration = 15
    # get end date
    x = end_date.split('-')
    x[0] = str(int(x[0]) - duration)
    start_date = '-'.join(x)

    # initialize APIs
    finn_api = FinnhubApi(
        base_url=config['api']['finnhub']['api_endpoint'],
        api_key=config['api']['finnhub']['token'],
        write_to_mongo=True,
        write_to_mysql=True,
        write_to_postgres=True,
        maria_conf=config['mariadb'],
        hadoop_conf=config['hadoop'],
        postgres_conf=config['postgres'],
        mongo_conf=config['mongodb'],
    )

    yf_api = YFApi(
        write_to_mongo=True,
        write_to_mysql=True,
        write_to_postgres=True,
        maria_conf=config['mariadb'],
        hadoop_conf=config['hadoop'],
        postgres_conf=config['postgres'],
        mongo_conf=config['mongodb'],
    )

    fmp_api = FmpApi(
        base_url=config['api']['fmp']['api_endpoint'],
        api_key=config['api']['fmp']['token'],
        write_to_mongo=True,
        write_to_mysql=True,
        write_to_postgres=True,
        maria_conf=config['mariadb'],
        hadoop_conf=config['hadoop'],
        postgres_conf=config['postgres'],
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
        # get historical data from yfinance, and save them in mysql and postgresql
        yf_api.fetch_historical_data(**yf_params)

        # get company news from finnhub api, and saved in mysql and postgresql
        finn_api.fin_api_req.fetch_company_news(params=finn_params)
        # get inside transactions, and saved in mongodb
        finn_api.fin_api_req.fetch_insider_transactions(params=finn_params)

        # get company related information and stored in mysql and postgresql
        fmp_api.fmp_api_req.fetch_company_ticker(params=fmp_params)
        fmp_api.fmp_api_req.fetch_company_profile(symbol=symbol)
        # get historical company rating and stored in msyql and postgresql
        fmp_api.fmp_api_req.fetch_historical_rating(symbol=symbol)
