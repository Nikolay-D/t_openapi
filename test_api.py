from pandas import DataFrame as df
from includes.openapi import Tinkoff
import time, os, sys, json

my_ticker = "LKOH"


def cls():
    print('\n' * 1024)


def myexit(some, code=0):
    print(f'Exit: {some}')
    os._exit(code)


def test(t):
    samples = 5
    # --- Show stocks ---

    stocks = t.get_stocks()
    tickers = stocks['ticker']

    len = stocks.len()
    print(f'\nStocks: (len: {len})')
    print(stocks.head(samples))
    keys = stocks.keys()
    print(f'stocks.keys() are: {keys}')

    for stock in stocks.rows()[0:6]:
        ticker = stock['ticker']
        ord = t.get_orderbook(ticker)
        trade_status = ord['tradeStatus']
        if trade_status == 'NormalTrading':
            print(f'{ticker}: orderbook: {ord}')
        else:
            print(f'{ticker} - {trade_status}')
        time.sleep(0.3)

    print(f'\nGet_position:')
    allpos = t.get_position()
    print(allpos)

    state = t.check_trading_state(my_ticker)
    print(f'Trading state: {state}')

    state = t.cancel_all_orders()
    print(f'Cancel state: {state}')

    # --- Show bonds ---
    bonds = t.get_bonds()
    len = bonds.len()
    print(f'\nBonds: (len: {len})')
    print(bonds.head(samples))

    # --- Show ETFs ---
    etfs = t.get_etfs()
    len = etfs.len()
    print(f'\nETF: (len: {len})')
    print(etfs.head(samples))

    # --- Show currencies ---
    currs = t.get_currencies()
    len = currs.len()
    print(f'\nGet_currencies: (len: {len})')
    print(currs.head(samples))
    keys = currs.keys()
    print(f'All keys are: {keys}')

    # --- Show portfolio ---

    pf = t.get_portfolio()
    print('\nGet_portfolio:')
    print(pf)

    pfcurr = t.get_portfolio_currencies()
    print('\nGet_portfolio_currencies:')
    print(pfcurr)
    pf_sum = round(pfcurr['size_rub'].sum(), 2)
    print(f'Sum currencies only: {pf_sum}')

    orders_table = t.get_orders_table()
    print(orders_table)
    res = orders_table.search_one({'ticker': my_ticker})
    print(f'Get orders table: {res}')
    order_id = res['orderId']
    if order_id:
        res = t.cancel_order(order_id)
        print(f'Cancel order: {order_id}')
    else:
        print(f'No orders for {my_ticker}')

    total_money, delta = t.get_total_balance()
    print(f'Total on Tinkoff: {total_money:,} {delta:+}')


##################################################

if __name__ == "__main__":
    with open('settings.json', 'r') as f:
        config = json.load(f)
    token = config['token']
    if not token:
        myexit('No token in settings.json!')
    t = Tinkoff(token)
    test(t)
    t.destroy()
    myexit('Done.')
