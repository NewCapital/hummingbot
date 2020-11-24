# coding=utf-8

import hashlib
import hmac
from urllib.parse import urlencode
import requests
import time


class NewcapitalClient(object):
    API_URL = 'https://api.new.capital'
    PUBLIC_API_VERSION = 'v1'
    PRIVATE_API_VERSION = 'v1'

    ORDER_STATUS_NEW = 'NEW'
    ORDER_STATUS_PARTIALLY_FILLED = 'PARTIALLY_FILLED'
    ORDER_STATUS_FILLED = 'FILLED'
    ORDER_STATUS_CANCELED = 'CANCELED'

    SIDE_BUY = 'BUY'
    SIDE_SELL = 'SELL'

    ORDER_TYPE_LIMIT = 'LIMIT'

    def __init__(self, api_key, api_secret):
        self.API_KEY = api_key
        self.API_SECRET = api_secret
        self.session = self._init_session()

    def _init_session(self):
        session = requests.session()
        session.headers.update({'Accept': 'application/json',
                                'User-Agent': 'new.capital/python',
                                'X-NC-APIKEY': self.API_KEY})
        return session

    def _create_api_uri(self, path, signed=True, version=PUBLIC_API_VERSION):
        v = self.PRIVATE_API_VERSION if signed else version
        return self.API_URL + '/' + v + '/' + path

    def _generate_signature(self, data):
        return hmac.new(self.API_SECRET.encode('utf-8'), urlencode(data).encode('utf-8'), hashlib.sha256).hexdigest()

    def _request(self, method, uri, signed, force_params=False, **kwargs):
        kwargs['timeout'] = 10
        data = kwargs.get('data', None)
        if data and isinstance(data, dict):
            kwargs['data'] = data

            if 'requests_params' in kwargs['data']:
                kwargs.update(kwargs['data']['requests_params'])
                del(kwargs['data']['requests_params'])

        if signed:
            kwargs['data']['nonce'] = int(time.time() * 100)
            self.session.headers.update({'X-NC-SIGNATURE': self._generate_signature(kwargs['data'])})

        if data and (method == 'get' or force_params):
            kwargs['params'] = kwargs['data']
            del(kwargs['data'])

        response = getattr(self.session, method)(uri, **kwargs)
        return self._handle_response(response)

    def _request_api(self, method, path, signed=False, version=PUBLIC_API_VERSION, **kwargs):
        uri = self._create_api_uri(path, signed, version)
        return self._request(method, uri, signed, **kwargs)

    def _handle_response(self, response):
        try:
            json_res = response.json()
        except ValueError:
            raise Exception('Invalid JSON error message from New Capital: {}'.format(response.text));
        if "code" in json_res:
            raise Exception(json_res["msg"]);
        return json_res

    def _get(self, path, signed=False, version=PUBLIC_API_VERSION, **kwargs):
        return self._request_api('get', path, signed, version, **kwargs)

    def _post(self, path, signed=False, version=PUBLIC_API_VERSION, **kwargs):
        return self._request_api('post', path, signed, version, **kwargs)

    def get_exchange_info(self):
        return self._get('exchangeInfo')

    def get_orderbook_tickers(self):
        return self._get('ticker')

    def get_order_book(self, **params):
        return self._get('depth', data=params)

    def get_recent_trades(self, **params):
        return self._get('trades', data=params)

    def get_ticker(self, **params):
        return self._get('ticker', data=params)

    def create_order(self, **params):
        return self._post('orders/new', True, data=params)

    def get_order(self, **params):
        return self._post('orders/get', True, data=params)

    def get_all_orders(self, **params):
        return self._post('orders/all', True, data=params)

    def cancel_order(self, **params):
        return self._post('orders/cancel', True, data=params)

    def get_open_orders(self, **params):
        return self._post('orders/list', True, data=params)

    def get_balances(self):
        return self._post('balances', True, data={})

    def get_asset_balance(self, asset):
        res = self.get_balances()
        for balance in res.items():
            if balance[0].upper() == asset.upper():
                return balance[1]["available"]
        return None
