import functools
import pandas as pd

from src.common.api_exception import ApiException
from src.common.mongodb import MongoConn


class ApiDecorator:

    @classmethod
    def inject_api_key(cls, param_api: str):
        def wrapper(func):
            @functools.wraps(func)
            def _call_wrapper(self, *args, **kwargs):
                api_key = self.api_key
                request = func(self, *args, **kwargs)
                if '?' not in request:
                    return request + f"?{param_api}=" + api_key
                else:
                    return request + f"&{param_api}=" + api_key
            return _call_wrapper
        return wrapper

    @classmethod
    def format_data(cls, func):
        @functools.wraps(func)
        def _call_wrapper(self, *args, **kwargs):
            response = func(self, *args, **kwargs)
            if self.output_format == 'json':
                return response.json()
            elif self.output_format == 'pandas':
                return pd.DataFrame(response.json())
            else:
                raise ApiException("Output must be either pandas or json",
                                   ApiDecorator.format_data.__name__)

        return _call_wrapper

    @classmethod
    def format_historical_data(cls, func):
        @functools.wraps(func)
        def _call_wrapper(self, *args, **kwargs):
            response = func(self, *args, **kwargs)
            resp = response.json()
            if self.output_format == 'json':
                return resp.get('historical', [])
            elif self.output_format == 'pandas':
                return pd.DataFrame(resp.get('historical', []))
            else:
                raise ApiException("Output must be either pandas or json",
                                   ApiDecorator.format_historical_data.__name__)

        return _call_wrapper

    @classmethod
    def write_to_mongodb(cls, db, col):
        def wrapper(func):
            @functools.wraps(func)
            def _call_wrapper(self, *args, **kwargs):
                response = func(self, *args, **kwargs)
                if self.write_to_mongo and response:
                    mongo_conn = MongoConn.initialize_mongodb_client(self.mongo_uri)
                    collection = mongo_conn[db][col]
                    collection.insert_many(response)
                return response

            return _call_wrapper
        return wrapper
