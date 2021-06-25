"""
 _   _       _          __  __        
| |_(_)_ __ | | _____  / _|/ _|       
| __| | '_ `| |/ / _ `| |_| |_        
| |_| | | | |   < (_) |  _|  _|       
 .__|_|_| |_|_|._.___/|_| |_|       _
  ___  _ __   ___ _ __   __ _ _ __ (_)
 / _ `| '_ ` / _ ` '_ ` / _` | '_ `| |
| (_) | |_) |  __/ | | | (_| | |_) | |
 .___/| .__/ .___|_| |_|.__,_| .__/|_|
      |_|                    |_|      
==============================================
Tinkoff OpenAPI by pmus@github
"""

import time, os, logging, sys, warnings, json
import requests as r
from collections import defaultdict as dd
from lomond import WebSocket
from lomond.persist import persist
from lomond.errors import WebSocketError
from pandas import DataFrame as df
from pandas import Series as series
from datetime import datetime, timezone, timedelta
from functools import wraps
from threading import Thread


def myasync(func):
    """ Useful async decorator """
    @wraps(func)
    def async_func(*args, **kwargs):
        func_hl = Thread(target=func, args=args, kwargs=kwargs)
        func_hl.daemon = False
        func_hl.start()
        return func_hl

    return async_func


class SuperDict(dict):
    def sorted(self) -> dict:
        res = {k: v for k, v in sorted(self.items(), key=lambda item: item[1])}
        return SuperDict(res)

    def sorted_by_key(self) -> dict:
        res = {k: v for k, v in sorted(self.items(), key=lambda item: item[0])}
        return SuperDict(res)

    def reverse_sorted_by_key(self) -> dict:
        res = {k: v for k, v in sorted(self.items(), key=lambda item: item[0], reverse=True)}
        return SuperDict(res)

    def filtered(self, callback) -> dict:
        res = {}
        for (key, value) in self.items():
            if callback((key, value)):
                res[key] = value
        return SuperDict(res)

    def firstkey(self) -> str:
        return tuple(self.keys())[0] if self.keys() else None

    def __call__(self) -> dict:
        return dict(self)

    def __repr__(self) -> str:
        return str(dict(self))

    def __missing__(self, key) -> None:
        return None

    def is_empty(self) -> bool:
        return len(self.keys()) == 0


class Table(df):
    def q(self, query) -> df:
        try:
            res = self.query(query)
        except KeyError:
            res = df()
            pass
        return Table(res)

    def sorted(self, key, asc=False) -> df:
        try:
            res = self.sort_values(by=key, ascending=asc)
        except:
            res = self
            pass
        return Table(res)

    def drop_key(self, key) -> df:
        try:
            res = Table(self.drop(key, 1))
        except KeyError:
            res = self
        return res

    def keys(self) -> list:
        return self.columns.tolist()

    def len(self) -> int:
        return len(self.index)

    def is_empty(self) -> bool:
        return self.len() < 1

    def search_dict(self, some_dict) -> df:
        res = []
        try:
            res = self.loc[self[list(some_dict.keys())].isin(list( \
                some_dict.values())).all(axis=1), :]
        except KeyError:
            pass  # Not found
        return Table(res)

    def search_one(self, some_dict) -> df:
        filtered = self.search_dict(some_dict)
        return filtered.last_value()

    def first_value(self) -> SuperDict:
        res = SuperDict()
        if self.len() > 0:
            res.update(self.iloc[0].to_dict())
        return res

    def last_value(self) -> SuperDict:
        res = SuperDict()
        if self.len() > 0:
            res.update(self.iloc[-1].to_dict())
        return res

    def rows(self) -> list:
        return self.to_dict('records')

    def get_mean(self, key) -> float:
        if key in self.keys():
            return self[key].mean()
        else:
            return 0

    def get_stdev(self, key) -> float:
        if key in self.keys():
            return self[key].std()
        else:
            return 0

    def get_sum(self, key):
        if key in self.keys():
            return self[key].sum()
        else:
            return 0

    def iter_rows(self):
        return [SuperDict(x) for x in self.to_dict('records')]

    def column(self, key):
        if key in self.keys():
            return self[key].values
        else:
            return []


methods = {
    'portfolio': {
        'method': 'get',
        'path': 'portfolio'
    },
    'portfolio/currencies': {
        'method': 'get',
        'path': 'portfolio/currencies'
    },
    'orders': {
        'method': 'get',
        'path': 'orders',
    },
    'orders/cancel': {
        'method': 'post',
        'path': 'orders/cancel',
    },
    'market/stocks': {
        'method': 'get',
        'path': 'market/stocks'
    },
    'market/bonds': {
        'method': 'get',
        'path': 'market/bonds'
    },
    'market/etfs': {
        'method': 'get',
        'path': 'market/etfs'
    },
    'market/currencies': {
        'method': 'get',
        'path': 'market/currencies'
    },
    'search/by-figi': {
        'method': 'get',
        'path': 'market/search/by-figi'
    },
    'search/by-ticker': {
        'method': 'get',
        'path': 'market/search/by-ticker'
    },
    'market/orderbook': {
        'method': 'get',
        'path': 'market/orderbook'
    },
    'market/candles': {
        'method': 'get',
        'path': 'market/candles'
    },
    'orders/limit-order': {
        'method': 'post',
        'path': 'orders/limit-order'
    },
    'orders/market-order': {
        'method': 'post',
        'path': 'orders/market-order'
    }
}


class Tinkoff(object):
    def __init__(self, secret, sleep_ms=30, subscribe=[], debug=False, ws_persist=True):
        """
        Tinkoff websocket connector.
        :param secret is a secret from Tinkoff
        :param sleep_ms is connector delay, default tp 10 ms
        """
        print(f'OpenAPI: Tinkoff connector started.')
        self.id = id(self)
        self.sleep_ms = sleep_ms
        self.rest_endpoint = "https://api-invest.tinkoff.ru/openapi"
        self.ws_endpoint = "wss://api-invest.tinkoff.ru/openapi/md/v1/md-openapi/ws"
        self.secret = secret
        self.connected = False
        self.run = True
        self.ready = False  # Ready to trade?
        self.debug = debug
        if not self.secret:
            raise RuntimeError('No secret key passed!')
        self.rest_session = r.Session()
        self.rest_session.headers.update({'Authorization': f'Bearer {self.secret}'})
        self.ws = None
        '''
        We implement REST now,
        so skip ws fn's for a while
        ============================
        self.ws = WebSocket(self.ws_endpoint, compress=True)
        self.ws_persist = ws_persist
        self.ws.on_disconnect = self.on_disconnect
        _auth = f'Bearer {self.secret}'
        self.ws.add_header("Authorization".encode('utf-8'), _auth.encode('utf-8'))
        if self.ws_persist:
            persist(self.ws, exit_event=self.on_disconnect)
        print(f'OpenAPI: Created websocket: {self.ws}')
        while not self.ws.is_active:
            print(f'OpenAPI: Waining for {self.ws} active state...')
            self.min_sleep()
        print(f'OpenAPI: {self.ws} is active now.')
        self.__loop__() 
        print(f'OpenAPI: Waiting for ready state...')
        while not self.ready:
            self.min_sleep()
        print(f'OpenAPI: Ready.')
        '''
        self.stocks = self.get_stocks()
        tickers = self.stocks['ticker']
        self.ticker_dict = SuperDict(zip(self.stocks['ticker'], self.stocks['figi']))
        self.figi_dict = SuperDict(zip(self.stocks['figi'], self.stocks['ticker']))

        self.currs = SuperDict()
        self.usd = 0
        self.eur = 0
        #currs = self.update_currencies()
        #print(f'$={self.usd} €={self.eur}')

    def update_currencies(self) -> SuperDict:
        """
        Updates self.currs dict, pulls from MOEX
        """
        eur_price, usd_price = 0, 0
        try:
            eur = self.get_orderbook_by_figi('BBG0013HJJ31')  # EUR
            eur_price = eur['closePrice']
            usd = self.get_orderbook_by_figi('BBG0013HGFT4')  # USD
            usd_price = usd['lastPrice']
        except:
            print('USING MOEX')
            url = "http://iss.moex.com/iss/engines/currency/markets/selt/boards/CETS/securities.json?iss.only=marketdata&marketdata.columns=SECID,LAST"
            reply = r.get(url).text
            currs = json.loads(reply)['marketdata']['data']
            cur_dict = {k[0]: k[1] for k in currs}
            usd_price = cur_dict['USD000UTSTOM']
            eur_price = cur_dict['EUR_RUB__TOM']
            pass
        self.usd = usd_price
        self.eur = eur_price
        return {'USD': usd_price, 'EUR': eur_price}

    def min_sleep(self):
        time.sleep(self.sleep_ms / 1000)

    def ticker_to_figi(self, ticker) -> str:
        res = self.ticker_dict[ticker]
        return res

    def figi_to_ticker(self, ticker) -> str:
        res = self.figi_dict[ticker]
        return res

    def get_path(self, method) -> tuple:
        _ = methods[method]
        method, path = _['method'], _['path']
        return method, path

    def connect(self) -> None:
        """ We connect to ws_endpoint here """
        ...
        self.connected = True

    def rest_send(self, method=None, params=None, body=None) -> SuperDict:
        """ Make request, return object """
        http_method, path = self.get_path(method)
        url = f'{self.rest_endpoint}/{path}'
        if params:
            url += '?'
            req = r.models.PreparedRequest()
            req.prepare_url(url, params)
            url = req.url

        http_res, http_code, http_text = None, None, None
        if http_method == 'get':
            http_res = self.rest_session.get(url)
        elif http_method == 'post':
            http_res = self.rest_session.post(url, data=body)
        else:
            raise ValueError(f'HTTP method unknown: {http_method}')

        http_code = http_res.status_code
        http_text = http_res.text

        if http_code == 200:
            answer = json.loads(http_text)
            res, tracking, status = answer['payload'], answer['trackingId'], answer['status']
            res = SuperDict(res) if (type(res) == dict) else res
            self.min_sleep()
            return res
        else:
            print(f'OpenAPI: rest_send: HTTP code:{http_code}, text:{http_text}')
            err_dict = {}
            try:
                err_dict = json.loads(http_text)['payload']
            except:
                pass
            return SuperDict(err_dict)

    def ws_send(self, request) -> None:
        ...

    def on_disconnect(self) -> None:
        print(f'OpenAPI: Tinkoff disconnected.')
        self.connected = False

    @myasync
    def __loop__(self) -> None:
        myevents = iter(self.ws)
        while self.run:
            self.min_sleep()
            event = None
            """ Check for connection """
            if not self.connected:
                self.connect()
            """ Connected, read incoming """
            try:
                event = next(myevents)
            except StopIteration:
                if self.debug:
                    print(f'OpenAPI: Iteration stopped.')
                pass

            if event:
                if event.name == 'ready':
                    self.ready = True
                if event.name in ['disconnected', 'connect_fail']:
                    self.ready = False
                if hasattr(event, 'text'):
                    incoming = SuperDict(event.json)
                    self.parse(incoming)
            else:
                print(f'OpenAPI: Renewing events...')
                myevents = iter(self.ws)
                self.min_sleep()

    def destroy(self) -> None:
        self.rest_session.close()
        if self.ws:
            self.ws.close()  # or: self.ws.state.session.close()
        self.run, self.ready = False, False

    def __del__(self):
        self.destroy()

    """
    Here we declare all connector functions
    =======================================
    [0] - Orders
    """

    def get_orders(self) -> list:
        """
        [{'orderId': '23731089759', 'figi': 'BBG004730ZJ9', 'operation': 'Buy', 'status': 'New', 'requestedLots': 1, 'executedLots': 0, 'price': 0.051585, 'type': 'Limit'}]
        """
        res = self.rest_send(method='orders', params=None)
        _ = []
        for order in res:
            order = SuperDict(order)
            order.update({'ticker': self.figi_to_ticker(order['figi'])})
            _.append(order.sorted_by_key())
        return _

    def get_orders_table(self) -> df:
        orders = self.get_orders()
        res = Table(orders)
        return res

    def post_limit_order(self) -> str:
        ...

    def post_market_order_figi(self, figi, lots, dir='buy') -> str:
        dir_dict = {'buy': 'Buy', 'sell': 'Sell'}
        dircode = dir_dict[dir]
        body = json.dumps({"lots": lots, "operation": dircode})
        res = self.rest_send(method='orders/market-order', params={'figi': figi}, body=body)
        return res

    def post_market_order(self, ticker, lots, dir='buy') -> str:
        """
        Returns:
        {'orderId': '23768877408', 'operation': 'Sell',
         'status': 'Fill', 'requestedLots': 1, 'executedLots': 1,
         'commission': {'currency': 'RUB', 'value': 0}}
        """
        figi = self.ticker_to_figi(ticker)
        res = self.post_market_order_figi(figi, lots, dir)
        return res

    def cancel_order(self, order_id) -> str:
        if not order_id:
            raise ValueError('Error, cancel_order got no order id!')
        res = self.rest_send(method='orders/cancel', params={'orderId': order_id})
        return res

    """ [1] - Portfolio """

    def get_portfolio(self) -> df:
        self.update_currencies()
        res = self.rest_send(method='portfolio', params=None)
        res_df = Table(res['positions'])
        return res_df

    def get_portfolio_currencies(self) -> df:
        self.update_currencies()
        res = []
        answer = self.rest_send(method='portfolio/currencies', params=None)
        for curr in answer['currencies']:
            coin = curr['currency']
            balance = curr['balance']
            size_rub = self.some_to_rub(coin, balance)
            curr.update({'size_rub': size_rub})
            res.append(curr)
        res_df = Table(res)
        return res_df

    def get_portfolio_stocks(self) -> df:
        pf = self.get_portfolio()
        res = pf.search_dict({'instrumentType': 'Stock'})
        return res

    def some_to_rub(self, coin, value) -> float:
        """
        Convert currency to rub +/-
        """
        usd = self.usd
        eur = self.eur
        if coin == 'RUB':
            res = value
        elif coin == 'USD':
            res = value * usd
        elif coin == 'EUR':
            res = value * eur
        else:
            raise ValueError(f'some_to_rub: Strange coin {coin} passed.')
        return round(res, 3)

    def _extend_position(self, res) -> SuperDict:
        errcode = 1  # When user trades in mobile app, error occurs.
        while errcode:
            try:
                balance = res['balance']
                lots = res['lots']
                avg = res['averagePositionPrice']['value']
                curr = res['averagePositionPrice']['currency']
                profit = res['expectedYield']['value']
                profit_rub = self.some_to_rub(curr, profit)
                size = (balance * avg) + profit
                price_cur = (size / balance)
                pct = (profit * 100) / size
                pre_size = price_cur * balance
                size_rub = self.some_to_rub(curr, pre_size)

                res.update({
                    'size': round(size, 3),
                    'price_avg': round(avg, 5),
                    'price_cur': round(price_cur, 5),
                    'profit': round(profit, 3),
                    'currency': curr,
                    'profit%': round(pct, 3),
                    'profit_rub': round(profit_rub, 3),
                    'size_rub': size_rub
                })

                del res['expectedYield']
                del res['averagePositionPrice']
                errcode = 0
            except Exception as e:
                print(f'_extend_position: error {e}, pass.')
                '''
                import traceback
                traceback.print_exc()
                '''
                raise
        return res.reverse_sorted_by_key()

    def get_position_by_ticker(self, ticker) -> SuperDict:
        stocks = self._get_portfolio_stocks()
        position = stocks.search_dict({'ticker': ticker})
        res = SuperDict()
        if position.len() > 0:
            res.update(position.rows[0])
            res = self._extend_position(res)
        return res

    def get_position(self) -> df:
        res = []
        position = self.get_portfolio()
        for row in position.rows():
            if row['instrumentType'].lower() == 'currency':
                continue
            try:
                extended = self._extend_position(SuperDict(row))
                if not extended:
                    extended = SuperDict({})
            except Exception as e:
                print('OpenAPI: failed to extend position.')
                extended = {}
                pass
            res.append(extended)
        return Table(res)

    def get_final_balance(self) -> float:
        ...

    """ [2] - Market """

    def get_stocks(self) -> df:
        res = self.rest_send(method='market/stocks', params=None)
        res_df = Table(res['instruments'])
        res_df.set_index('ticker')
        return res_df

    def get_bonds(self) -> df:
        res = self.rest_send(method='market/bonds', params=None)
        res_df = Table(res['instruments'])
        res_df.set_index('ticker')
        return res_df

    def get_etfs(self) -> df:
        res = self.rest_send(method='market/etfs', params=None)
        res_df = Table(res['instruments'])
        res_df.set_index('ticker')
        return res_df

    def get_currencies(self) -> df:
        res = self.rest_send(method='market/currencies', params=None)
        res_df = Table(res['instruments'])
        res_df.set_index('ticker')
        return res_df

    def get_orderbook_by_figi(self, figi) -> SuperDict:
        res = self.rest_send(method='market/orderbook', params={'figi': figi, 'depth': 20})
        return res

    def get_orderbook(self, ticker) -> SuperDict:
        """
        Param: ticker required
        Result: dict, keys are : ['figi', 'depth', 'tradeStatus',
        'minPriceIncrement', 'lastPrice', 'closePrice', 'limitUp',
        'limitDown', 'bids', 'asks'].
        ['bids'] and ['asks'] are Tables.
        """
        figi = self.ticker_to_figi(ticker)
        if not figi:
            warnings.warn(f'No figi found for {ticker}')
            return SuperDict()
        try:
            res = SuperDict(self.get_orderbook_by_figi(figi))
        except:
            print(f'Error for {ticker}, got {by_figi}')
            res = SuperDict()
            pass
        return res

    def get_orderbook_tables(self, ticker) -> df:
        res = self.get_orderbook(ticker)
        to_return = SuperDict(res)
        if res['bids'] and res['asks']:
            bids = Table(res['bids']).sort_values(by='price', ascending=False)
            asks = Table(res['asks']).sort_values(by='price', ascending=False)
            to_return['bids'] = bids
            to_return['asks'] = asks
        return to_return

    def get_candles_by_figi(self, figi, timestamp_from=0, timestamp_to=0, interval='day') -> df:
        """
        Intervals are:
        ==============
        1min, 2min, 3min, 5min, 10min, 15min,
        30min, hour, day, week, month

        NB: It works only from day and up.
        --- questions to Tinkoff ---
        """
        if not timestamp_to:
            timestamp_to = int(datetime.now().timestamp())
        if not timestamp_from:
            timestamp_from = int(timestamp_to - timedelta(days=365).total_seconds())
        str_time_from = datetime.fromtimestamp(timestamp_from, timezone.utc).isoformat()
        str_time_to = datetime.fromtimestamp(timestamp_to, timezone.utc).isoformat()
        res = self.rest_send(method='market/candles',
                             params={
                                 'figi': figi,
                                 'from': str_time_from,
                                 'to': str_time_to,
                                 'interval': interval
                             })

        list_candles = SuperDict(res)['candles']
        ticker = self.figi_to_ticker(figi)
        candles = []
        for row in list_candles:
            candle = SuperDict(row)
            candle.update({'ticker': ticker})
            candles.append(candle)
        res = Table(candles)
        return res

    def get_candles(self, ticker, timestamp_from=0, timestamp_to=0, interval='day') -> df:
        figi = self.ticker_to_figi(ticker)
        res = self.get_candles_by_figi(figi, timestamp_from, timestamp_to, interval)
        return res

    def search_by_figi(self, figi) -> SuperDict:
        res = self.rest_send(method='search/by-figi', params={'figi': figi})
        return res

    def search_by_ticker(self, ticker) -> SuperDict:
        res = self.rest_send(method='search/by-ticker', params={'ticker': ticker})
        res_list = res['instruments']
        return SuperDict(res_list[0]) if res_list else SuperDict()

    def cancel_all_orders(self) -> None:
        orders = self.get_orders()
        if not orders:
            return
        for order in orders:
            order_id = order['orderId']
            print(f'Cancelling order: {order_id}')
            state = self.cancel_order(order_id)
        return

    """ [3] - Operations """

    def get_operations(self, timestamp_from=0, timestamp_to=0) -> df:
        ...

    """ [4] - User """

    def get_user_accounts(self) -> df:
        raise NotImplementedError('get_user_accounts: not implemented yet.')

    def check_trading_state(self, ticker='LKOH') -> bool:
        orderbook = self.get_orderbook(ticker)
        trade_status = orderbook['tradeStatus']
        return trade_status == 'NormalTrading'

    def get_total_balance(self) -> tuple:
        allpos = self.get_position()
        if allpos.is_empty():
            pos_sum = 0
            profit_sum = 0
        else:
            try:
                pos_sum = round(allpos['size_rub'].sum(), 2)
                profit_sum = round(allpos['profit_rub'].sum(), 2)
            except KeyError as k:
                print(f'Error in get_total_balance {k}')
                pos_sum = 0
                profit_sum = 0
                pass
        """ This value is profit for all time holding position """
        pfcurr = self.get_portfolio_currencies()
        try:
            pf_sum = round(pfcurr['size_rub'].sum(), 2)
            total_sum = round(pos_sum + pf_sum, 2)
            if total_sum > 0:
                pos_delta = round((profit_sum * 100) / total_sum, 2)
            else:
                pos_delta = 0
        except KeyError:
            total_sum = 0
            pos_delta = 0
        return total_sum, pos_delta


if __name__ == '__main__':
    print(f'OpenAPI: This is a module, it starts not here.')