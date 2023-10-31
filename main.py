import asyncio
import json
from datetime import datetime
import websockets
import pandas as pd
from tabulate import tabulate


df = pd.DataFrame(columns=['Time', 'Exchange', 'Spot', 'Future', 'Basis'])


# Create class instance to store data
class ExchangeData:
    def __init__(self, name):
        self.exchange_name = name
        self.spot_price = None
        self.spot_time = None
        self.future_price = None
        self.future_time = None

    # Handling data with different formats
    def update_data(self, data):
        if self.exchange_name == 'Binance':
            if 'e' in data:
                if data['e'] == 'aggTrade':
                    self.spot_price = data['p']
                    self.spot_time = data['E']
                elif data['e'] == 'continuous_kline':
                    self.future_price = data['k']['c']
                    self.future_time = data['k']['T']
        elif self.exchange_name == 'Bybit':
            if 'topic' in data:
                if data['topic'] == 'tickers.BTCUSDT':
                    self.spot_time = data['ts']
                    self.spot_price = data['data']['lastPrice']
                else:
                    self.future_time = data['ts']
                    self.future_price = data['data']['lastPrice']
        elif self.exchange_name == 'Okx':
            if 'data' in data:
                if data['arg']['instId'] == 'BTC-USDT':
                    self.spot_time = data['data'][0]['ts']
                    self.spot_price = data['data'][0]['last']
                else:
                    self.future_time = data['data'][0]['ts']
                    self.future_price = data['data'][0]['last']

    # Both future and spot prices are ready
    def is_ready(self):
        return None not in [self.spot_price, self.spot_time, self.future_price, self.future_time]

    def reset(self):
        self.spot_price, self.spot_time, self.future_price, self.future_time = None, None, None, None


async def handler(uri, unique_request, exchange_name, exchanges) -> None:
    async with websockets.connect(uri) as ws:
        if unique_request != '':
            await ws.send(json.dumps(unique_request))
        try:
            while True:
                message = await ws.recv()
                data = json.loads(message)
                # print(data)
                exchange_data = exchanges[exchange_name]
                exchange_data.update_data(data)
                if exchange_data.is_ready():
                    update_data(exchange_data)

                # Only extract the first response/stream with useful data
                if 'topic' in data or 'e' in data or 'data' in data:
                    break
        except websockets.ConnectionClosed:
            print(f"Connection to {uri} closed")
        except Exception as e:
            print(f"An error occurred: {e}")


def update_data(exchange) -> None:
    global df
    spot_price = float(exchange.spot_price)
    future_price = float(exchange.future_price)
    basis = future_price - spot_price
    t_time = datetime.fromtimestamp(
        max(int(exchange.spot_time), int(exchange.future_time)) / 1000
    ).strftime("%Y-%m-%d %H:%M")

    # add new data to the existing DataFrame
    df.loc[len(df)] = {
        'Time': t_time,
        'Exchange': exchange.exchange_name,
        'Spot': round(spot_price, 2),
        'Future': round(future_price, 2),
        'Basis': round(basis, 2)
    }
    display_data()
    exchange.reset()


def display_data():
    global df
    print(chr(27) + "[2J")
    print(tabulate(df.tail(9), headers='keys', tablefmt='pretty', showindex=False))
    df.to_csv('./monitor_log.csv')


async def main():
    global df
    data_instance = {name: ExchangeData(name) for name in ['Binance', 'Bybit', 'Okx']}

    # Create DataFrame that hold all the information
    symbol = 'BTCUSDT'

    # Aggregate all the configs
    exchange_configs = [
        {'uri': f"wss://fstream.binance.com/ws/{symbol.lower()}_current_quarter@continuousKline_1m",
         'request': '', 'name': 'Binance'},
        {'uri': f"wss://stream.binance.com:9443/ws/{symbol.lower()}@aggTrade",
         'request': '', 'name': 'Binance'},
        {'uri': f"wss://stream.bybit.com/v5/public/spot",
         'request': {"op": "subscribe", "args": [f"tickers.{symbol}"]}, 'name': 'Bybit'},
        {'uri': f"wss://stream.bybit.com/v5/public/inverse",
         'request': {"op": "subscribe", "args": [f"tickers.{symbol[:-1]}Z23"]}, 'name': 'Bybit'},
        {'uri': f"wss://ws.okx.com:8443/ws/v5/public",
         'request': {"op": "subscribe", "args": [{"channel": "tickers", "instId": f"{symbol[:3] + '-' + symbol[3:]}"}]}, 'name': 'Okx'},
        {'uri': f"wss://ws.okx.com:8443/ws/v5/public",
         'request': {"op": "subscribe", "args": [{"channel": "tickers", "instId": f"{symbol[:3] + '-' + symbol[3:] + '-231229'}"}]}, 'name': 'Okx'},
    ]
    # Live Monitoring
    while True:
        tasks = [handler(config['uri'], config['request'], config['name'], data_instance) for config in exchange_configs]
        await asyncio.gather(*tasks)
        await asyncio.sleep(45)     # 15 secs for buffering

if __name__ == "__main__":
    asyncio.run(main())
