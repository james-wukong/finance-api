import requests
from requests import Response

from src.common.api_exception import ApiException
from src.common.decorator import ApiDecorator
from src.common.req_compiler import FmpRequest
from src.common.interface import ApiInterface


class FmpApi(ApiInterface):
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
                 maria_conf: dict = None,
                 azure_conf: dict = None,
                 postgres_conf: dict = None):
        super(FmpApi, self).__init__(base_url=base_url,
                                     api_key=api_key,
                                     output_format=output_format,
                                     write_to_mysql=write_to_mysql,
                                     write_to_postgres=write_to_postgres,
                                     write_to_azure=write_to_azure,
                                     write_to_mongo=write_to_mongo,
                                     mongo_conf=mongo_conf,
                                     maria_conf=maria_conf,
                                     azure_conf=azure_conf,
                                     postgres_conf=postgres_conf)
        self.fmp_api_req = FmpRequest(base_url=self.base_url,
                                      api_key=self.api_key)

    @ApiDecorator.write_to_mariadb
    def get_company_ticker(self, params=None):
        """
        get company ticker info from FMP api: search-ticker
        :param params: dict, such as, {'symbol': 'TSLA', 'from':'2000-01-01'}
        :return:
        """
        api_uri = self.fmp_api_req.compile_request(category='v3/search-ticker',
                                                   params=params)
        company = requests.get(api_uri)
        if not company.ok:
            raise ApiException("response from finnhub api is not OK",
                               FmpApi.get_company_ticker.__name__)

        # statement used to upsert data into database
        stmt = ("INSERT INTO company_ticker "
                "(symbol, name, currency, stock_exchange, exchange_short) "
                "VALUES (%s, %s, %s, %s, %s) AS new "
                "ON DUPLICATE KEY UPDATE name=new.name, "
                "currency=new.currency, exchange_short=new.exchange_short")
        return [(item['symbol'], item['name'], item['currency'],
                 item['stockExchange'], item['exchangeShortName'])
                for item in company.json()], stmt

    @ApiDecorator.write_to_mariadb
    def get_company_profile(self, category: str = None):
        """
        get company information from FMP api: company profile
        :param category: str, such as symbol 'TSLA'
        :return:
        """
        api_uri = self.fmp_api_req.compile_request(category=f'v3/profile/{category}')
        company = requests.get(api_uri)
        if not company.ok:
            raise ApiException("response from finnhub api is not OK",
                               FmpApi.get_company_profile.__name__)

        # statement used to upsert data into database
        stmt = ("INSERT INTO company_profile "
                "(symbol, name, currency, stock_exchange, exchange_short, "
                "price, beta, vol_avg, mkt_cap, last_div, range_1, changes, cik, "
                "isin, cusip, industry, website, description, ceo, sector, "
                "country, fulltime_employees, phone, address, city, state, "
                "zip, dcf_diff, dcf, image, ipo_date, default_image, "
                "is_etf, is_active_trading, is_adr, is_fund) "
                "VALUES (%s, %s, %s, %s, %s, %s, "
                "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,"
                "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)  AS new "
                "ON DUPLICATE KEY UPDATE name=new.name, currency=new.currency, price=new.price, "
                "beta=new.beta, vol_avg=new.vol_avg, mkt_cap=new.mkt_cap, last_div=new.last_div, "
                "range_1=new.range_1, changes=new.changes, cik=new.cik, isin=new.isin, cusip=new.cusip, "
                "industry=new.industry, website=new.website, description=new.description, ceo=new.ceo, "
                "sector=new.sector, country=new.country, fulltime_employees=new.fulltime_employees, "
                "phone=new.phone, address=new.address, city=new.city, state=new.state, zip=new.zip, "
                "dcf_diff=new.dcf_diff, dcf=new.dcf, image=new.image, ipo_date=new.ipo_date, is_etf=new.is_etf, "
                "default_image=new.default_image, is_active_trading=new.is_active_trading, "
                "is_adr=new.is_adr, is_fund=new.is_fund")
        return [(item['symbol'], item['companyName'], item['currency'],
                 item['exchange'], item['exchangeShortName'],
                 item['price'], item['beta'], item['volAvg'], item['mktCap'],
                 item['lastDiv'], item['range'], item['changes'], item['cik'],
                 item['isin'], item['cusip'], item['industry'], item['website'],
                 item['description'], item['ceo'], item['sector'], item['country'],
                 int(item['fullTimeEmployees']), item['phone'], item['address'], item['city'],
                 item['state'], item['zip'], item['dcfDiff'], item['dcf'],
                 item['image'], item['ipoDate'], item['defaultImage'], item['isEtf'],
                 item['isActivelyTrading'], item['isAdr'], item['isFund'])
                for item in company.json()], stmt

    @ApiDecorator.write_to_maria_sp(write_table='company_ticker')
    @ApiDecorator.write_to_postgres_sp(write_table='company_ticker')
    @ApiDecorator.write_to_mongodb_sp(collection='company_ticker')
    def fetch_company_ticker(self, params: dict = None) -> Response:
        """
        get company ticker info from FMP api: search-ticker
        :param params: dict, such as, {query: (str), limit: (int), exchange: (str)}
        :return:
        """
        api_uri = self.fmp_api_req.compile_request(category='v3/search-ticker',
                                                   params=params)
        ticker = requests.get(api_uri)
        if not ticker.ok:
            raise ApiException("response from finnhub api is not OK",
                               FmpApi.get_company_ticker.__name__)

        return ticker

    @ApiDecorator.write_to_maria_sp(write_table='company_profile')
    @ApiDecorator.write_to_postgres_sp(write_table='company_profile')
    @ApiDecorator.write_to_mongodb_sp(collection='company_profile')
    def fetch_company_profile(self, category: str = None) -> Response:
        """
        get company information from FMP api: company profile
        :param category: str, such as symbol 'TSLA'
        :return:
        """
        api_uri = self.fmp_api_req.compile_request(category=f'v3/profile/{category}')
        company = requests.get(api_uri)
        if not company.ok:
            raise ApiException("response from finnhub api is not OK",
                               FmpApi.fetch_company_profile.__name__)

        return company

    @ApiDecorator.write_to_mongodb_sp(collection='company_daily_chart')
    def fetch_daily_chart(self, category: str = None) -> Response:
        """
        get company information from FMP api: company profile
        :param category: str, such as symbol 'TSLA'
        :return:
        """
        api_uri = self.fmp_api_req.compile_request(category=f'v3/historical-price-full/{category}')
        chart = requests.get(api_uri)
        if not chart.ok:
            raise ApiException("response from finnhub api is not OK",
                               FmpApi.fetch_daily_chart.__name__)

        return chart

    def fetch_stock_news(self, params: dict = None):
        """
        get company information from FMP api: company profile
        :param params: {page: (int), tickers: (str), limit(int)}
        :return:
        """
        api_uri = self.fmp_api_req.compile_request(category='v3/stock_news',
                                                   params=params)
        news = requests.get(api_uri)
        if not news.ok:
            raise ApiException("response from finnhub api is not OK",
                               FmpApi.fetch_stock_news.__name__)

        return news
