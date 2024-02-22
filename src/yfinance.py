import yfinance as yf

from src.common.decorator import ApiDecorator
from src.common.interface import ApiInterface


class YFApi(ApiInterface):
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
        super(YFApi, self).__init__(base_url=base_url,
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

    @ApiDecorator.write_to_maria_sp(write_table='yf_historical_data')
    @ApiDecorator.write_to_postgres_sp(write_table='yf_historical_data')
    # @ApiDecorator.write_to_mongodb_sp(collection='yf_historical_data')
    def fetch_historical_data(self, symbol: str = '', start: str = '', end: str = ''):
        df = yf.download(symbol,
                         start=start,
                         end=end,
                         progress=False,
                         )
        df.reset_index(inplace=True)
        return df
