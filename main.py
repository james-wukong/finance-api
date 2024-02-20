import yaml

from src.finnhub import FinnhubApi
from src.fmp import FmpApi

if __name__ == '__main__':
    with open('conf.yaml', 'r') as file:
        config = yaml.safe_load(file)

    fin_api = FinnhubApi(base_url=config['api']['finnhub']['api_endpoint'],
                         api_key=config['api']['finnhub']['token'],
                         write_to='mongo',
                         mongo_conf=config['mongodb'])

    news_params = {'symbol': 'AAPL', 'from': '2023-08-15', 'to': '2024-01-20'}
    # get company news and save in mongodb
    news = fin_api.get_company_news(params=news_params)

    insider_params = {
        'symbol': 'AAPL',
        'from': '2023-08-15',
        'to': '2024-01-20'
    }
    # get insider transactions and save in mongodb
    insider = fin_api.get_insider_transactions(params=insider_params)

    fmp_api = FmpApi(base_url=config['api']['fmp']['api_endpoint'],
                     api_key=config['api']['fmp']['token'],
                     write_to='mariadb',
                     mariadb_conf=config['mariadb'])
    fmp_params = {'query': 'AA', 'limit': 10, 'exchange': 'NASDAQ'}
    # get company ticker and save in mariadb
    companies = fmp_api.get_company_ticker(params=fmp_params)
    category = 'v3/profile/TSLA'
    company = fmp_api.get_company_profile(category=category)
