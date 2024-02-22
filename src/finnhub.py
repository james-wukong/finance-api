from __future__ import annotations

import requests
from requests import Response

from src.common.req_compiler import FinnhubRequest
from src.common.decorator import ApiDecorator
from src.common.api_exception import ApiException
from src.common.interface import ApiInterface


class FinnhubApi(ApiInterface):
    """
    Base class that implements api calls
    """

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
        super(FinnhubApi, self).__init__(base_url=base_url,
                                         api_key=api_key,
                                         output_format=output_format,
                                         write_to_mysql=write_to_mysql,
                                         write_to_postgres=write_to_postgres,
                                         write_to_azure=write_to_azure,
                                         write_to_mongo=write_to_mongo,
                                         mongo_conf=mongo_conf,
                                         hadoop_conf=hadoop_conf,
                                         maria_conf=maria_conf,
                                         azure_conf=azure_conf,
                                         postgres_conf=postgres_conf)
        self.fin_api_req = FinnhubRequest(base_url=self.base_url,
                                          api_key=self.api_key)

    @ApiDecorator.format_data
    def get_company_news(self, params: dict = None):
        api_uri = self.fin_api_req.compile_request(category='company-news',
                                                   params=params)
        news = requests.get(api_uri)
        if not news.ok:
            raise ApiException("response from finnhub api is not OK",
                               FinnhubApi.get_company_news.__name__)
        return news

    def get_insider_transactions(self, params: dict = None):
        api_uri = self.fin_api_req.compile_request(
            category='stock/insider-transactions', params=params)
        insider = requests.get(api_uri)
        if not insider.ok:
            raise ApiException("response from finnhub api is not OK",
                               FinnhubApi.get_insider_transactions.__name__)

        return insider.json()['data']

    @ApiDecorator.write_to_maria_sp(write_table='finn_company_news')
    @ApiDecorator.write_to_postgres_sp(write_table='finn_company_news')
    # @ApiDecorator.write_to_mongodb_sp(collection='finn_company_news')
    def fetch_company_news(self, params: dict = None) -> Response:
        """
        get company news
        :param params: {symbol: (str), from: (date), to: (date) }
        :return:
        """
        api_uri = self.fin_api_req.compile_request(category='company-news',
                                                   params=params)
        print(api_uri)
        news = requests.get(api_uri)
        if not news.ok:
            raise ApiException("response from finnhub api is not OK",
                               FinnhubApi.fetch_company_news.__name__)
        return news

    @ApiDecorator.write_to_mongodb_sp(collection='finn_insider_transactions')
    def fetch_insider_transactions(self, params: dict = None) -> Response:
        """
        get insider transactions
        :param params: {symbol: (str), from: (date), to: (date) }
        :return:
        """
        api_uri = self.fin_api_req.compile_request(
            category='stock/insider-transactions', params=params)
        insider = requests.get(api_uri)
        if not insider.ok:
            raise ApiException("response from finnhub api is not OK",
                               FinnhubApi.fetch_insider_transactions.__name__)

        return insider
