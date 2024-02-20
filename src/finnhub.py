from __future__ import annotations

import requests
from datetime import datetime

from src.common.req_compiler import FinnhubRequest
from src.common.decorator import ApiDecorator
from src.common.api_exception import ApiException


class FinnhubApi:
    """
    Base class that implements api calls
    """

    def __init__(self,
                 base_url=None,
                 api_key=None,
                 output_format='json',
                 write_to='mongo',
                 mongo_conf=None):
        self.base_url = base_url
        self.api_key = api_key
        self.output_format = output_format
        self.write_to = write_to
        self.mongo_conf = mongo_conf
        self.mongo_uri = self.generate_mongo_uri()
        self.current_day = datetime.today().strftime('%Y-%m-%d')
        self.fin_api_req = FinnhubRequest(base_url=self.base_url,
                                          api_key=self.api_key)

    def generate_mongo_uri(self) -> str | None:
        if not self.mongo_conf:
            return None
        return (f"mongodb+srv://{self.mongo_conf['user']}:"
                f"{self.mongo_conf['token']}@"
                f"{self.mongo_conf['host']}"
                f"/?retryWrites=true&w=majority")

    @ApiDecorator.write_to_mongodb(db='finance_api', col='stock_news')
    @ApiDecorator.format_data
    def get_company_news(self, params=None):
        api_uri = self.fin_api_req.compile_request(category='company-news',
                                                   params=params)
        news = requests.get(api_uri)
        if not news.ok:
            raise ApiException("response from finnhub api is not OK",
                               FinnhubApi.get_company_news.__name__)
        return news

    @ApiDecorator.write_to_mongodb(db='finance_api', col='insider_transactions')
    def get_insider_transactions(self, params=None):
        api_uri = self.fin_api_req.compile_request(
            category='stock/insider-transactions', params=params)
        insider = requests.get(api_uri)
        if not insider.ok:
            raise ApiException("response from finnhub api is not OK",
                               FinnhubApi.get_insider_transactions.__name__)

        return insider.json()['data']
