import yfinance as yf

from src.common.decorator import ApiDecorator
from src.common.baseapi import BaseApi


class YFBaseApi(BaseApi):
    """
    Base class that implements api calls
    """
    # @ApiDecorator.write_to_hadoop_csv(file_name='yf_historical_data')
    @ApiDecorator.write_to_mssql_sp(write_table='yf_historical_data')
    @ApiDecorator.write_to_maria_sp(write_table='yf_historical_data')
    @ApiDecorator.write_to_postgres_sp(write_table='yf_historical_data')
    # @ApiDecorator.write_to_mongodb_sp(collection='yf_historical_data')
    def fetch_historical_data(self, symbol: str = '', start: str = '', end: str = ''):
        df = yf.download(symbol,
                         start=start,
                         end=end,
                         progress=False,
                         )
        df['symbol'] = symbol
        df.reset_index(inplace=True)
        return df
