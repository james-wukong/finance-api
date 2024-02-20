from datetime import datetime

import requests

from src.common.api_exception import ApiException
from src.common.decorator import ApiDecorator
from src.common.req_compiler import FmpRequest


class FmpApi:
    """
    Base class that implements api calls
    """

    def __init__(self,
                 base_url=None,
                 api_key=None,
                 output_format='json',
                 write_to='mariadb',
                 mariadb_conf=None):
        self.base_url = base_url
        self.api_key = api_key
        self.output_format = output_format
        self.write_to = write_to
        self.mariadb_conf = mariadb_conf
        self.current_day = datetime.today().strftime('%Y-%m-%d')
        self.fmp_api_req = FmpRequest(base_url=self.base_url,
                                      api_key=self.api_key)

    @ApiDecorator.write_to_mariadb
    def get_company_ticker(self, params=None):
        api_uri = self.fmp_api_req.compile_request(category='v3/search-ticker',
                                                   params=params)
        company = requests.get(api_uri)
        if not company.ok:
            raise ApiException("response from finnhub api is not OK",
                               FmpApi.get_company_ticker.__name__)

        stmt = ("INSERT INTO company_ticker "
                "(symbol, name, currency, stock_exchange, exchange_short) "
                "VALUES (%s, %s, %s, %s, %s)")
        return [(item['symbol'], item['name'], item['currency'],
                 item['stockExchange'], item['exchangeShortName'])
                for item in company.json()], stmt
