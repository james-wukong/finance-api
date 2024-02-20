from abc import ABC, abstractmethod
from collections import defaultdict
from src.common.decorator import ApiDecorator
from src.common.api_exception import ApiException


class RequestBuilder(ABC):

    def __init__(self, base_url, api_key):
        self.api_key = api_key
        self.__base_url = base_url
        self.__category = None
        self.__query_params = None

    def __build_category(self):
        if self.__category is None:
            raise ApiException("Category should not be empty !")
        return ''.join(['/', self.__category])

    def __build_query_params(self):
        if len(self.__query_params) == 0:
            return ''

        query_params_dict = self.__query_params.copy()

        query_string = '?'
        while len(query_params_dict) > 0:
            item = query_params_dict.popitem()
            pair_string = ''.join([item[0], '=', str(item[1])])
            query_string = ''.join([query_string, pair_string])

            if len(query_params_dict) != 0:
                query_string = ''.join([query_string, '&'])

        return query_string

    @abstractmethod
    def compile_request(self, category: str = None, params: dict = defaultdict):
        self.__category = category
        self.__query_params = params
        category = self.__build_category()
        query_params = self.__build_query_params()

        return ''.join([self.__base_url, category, query_params])


class FinnhubRequest(RequestBuilder):

    @ApiDecorator.inject_api_key(param_api='token')
    def compile_request(self, category: str = None, params: dict = defaultdict):
        return super(FinnhubRequest, self).compile_request(category, params)


class FmpRequest(RequestBuilder):

    @ApiDecorator.inject_api_key(param_api='apikey')
    def compile_request(self, category: str = None, params: dict = defaultdict):
        return super(FmpRequest, self).compile_request(category, params)
