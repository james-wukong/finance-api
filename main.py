import yaml

from src.finnhub import FinnhubApi

if __name__ == '__main__':
    with open('conf.yaml', 'r') as file:
        config = yaml.safe_load(file)

    fin_api = FinnhubApi(base_url=config['api']['finnhub']['api_endpoint'],
                         api_key=config['api']['finnhub']['token'],
                         write_to_mongo=True,
                         mongo_conf={
                             'host': config['mongodb']['host'],
                             'user': config['mongodb']['user'],
                             'pwd': config['mongodb']['pwd'],
                             'token': config['mongodb']['token'],
                         })

    params = {'symbol': 'AAPL',
              'from': '2023-08-15',
              'to': '2024-01-20'}
    # news = fin_api.get_company_news(params=params)

    insider = fin_api.get_insider_transactions(params=params)
