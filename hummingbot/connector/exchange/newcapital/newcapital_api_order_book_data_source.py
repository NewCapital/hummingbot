#!/usr/bin/env python

import asyncio
import aiohttp
import logging
import pandas as pd
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
    Optional
)
from decimal import Decimal
import re
import requests
import cachetools.func
import time
import ujson
import websockets
from websockets.exceptions import ConnectionClosed

from hummingbot.core.utils.async_utils import safe_gather
from hummingbot.core.data_type.order_book_tracker_data_source import OrderBookTrackerDataSource
from hummingbot.core.data_type.order_book_message import OrderBookMessage
from hummingbot.core.data_type.order_book import OrderBook
from hummingbot.logger import HummingbotLogger
from hummingbot.connector.exchange.newcapital.newcapital_order_book import NewcapitalOrderBook
from hummingbot.connector.exchange.newcapital.newcapital_utils import convert_to_exchange_trading_pair
from hummingbot.connector.exchange.newcapital.newcapital_utils import convert_from_exchange_trading_pair

SNAPSHOT_REST_URL = "https://api.new.capital/v1/depth"
TICKER_PRICE_CHANGE_URL = "https://api.new.capital/v1/ticker"
EXCHANGE_INFO_URL = "https://api.new.capital/v1/exchangeInfo"
WS_URL = "wss://new.capital/exchange/websocket"


class NewcapitalAPIOrderBookDataSource(OrderBookTrackerDataSource):

    MESSAGE_TIMEOUT = 30.0
    PING_TIMEOUT = 10.0

    _baobds_logger: Optional[HummingbotLogger] = None

    @classmethod
    def logger(cls) -> HummingbotLogger:
        if cls._baobds_logger is None:
            cls._baobds_logger = logging.getLogger(__name__)
        return cls._baobds_logger

    def __init__(self, trading_pairs: List[str]):
        super().__init__(trading_pairs)
        self._order_book_create_function = lambda: OrderBook()

    @classmethod
    async def get_last_traded_prices(cls, trading_pairs: List[str]) -> Dict[str, float]:
        try:
            from hummingbot.connector.exchange.newcapital.newcapital_utils import convert_from_exchange_trading_pair

            async with aiohttp.ClientSession() as client:
                async with client.get(TICKER_PRICE_CHANGE_URL, timeout=10) as response:
                    if response.status == 200:
                        tickers = await response.json()
                        last_traded_prices = {}
                        for ticker in tickers:
                            symbol = convert_from_exchange_trading_pair(ticker['symbol'])
                            if symbol in trading_pairs:
                                last_traded_prices[symbol] = Decimal(ticker.get("lastPrice", "0"))
                        for symbol in trading_pairs:
                            if symbol not in last_traded_prices:
                                last_traded_prices[symbol] = 0.0
                        return last_traded_prices
        except Exception:
            pass

        return {}

    @staticmethod
    @cachetools.func.ttl_cache(ttl=10)
    def get_mid_price(trading_pair: str) -> Optional[Decimal]:
        from hummingbot.connector.exchange.newcapital.newcapital_utils import convert_to_exchange_trading_pair

        resp = requests.get(url=f"{TICKER_PRICE_CHANGE_URL}?symbol={convert_to_exchange_trading_pair(trading_pair)}")
        record = resp.json()
        result = (Decimal(record.get("bidPrice", "0")) + Decimal(record.get("askPrice", "0"))) / Decimal("2")
        return result if result else None

    @staticmethod
    async def fetch_trading_pairs() -> List[str]:
        try:
            from hummingbot.connector.exchange.newcapital.newcapital_utils import convert_from_exchange_trading_pair

            async with aiohttp.ClientSession() as client:
                async with client.get(EXCHANGE_INFO_URL, timeout=10) as response:
                    if response.status == 200:
                        data = await response.json()
                        raw_trading_pairs = [d["symbol"] for d in data["symbols"]]
                        trading_pair_list: List[str] = []
                        for raw_trading_pair in raw_trading_pairs:
                            converted_trading_pair: Optional[str] = \
                                convert_from_exchange_trading_pair(raw_trading_pair)
                            if converted_trading_pair is not None:
                                trading_pair_list.append(converted_trading_pair)
                        return trading_pair_list

        except Exception:
            pass

        return []

    @staticmethod
    async def get_snapshot(client: aiohttp.ClientSession, trading_pair: str, limit: int = 1000) -> Dict[str, Any]:
        params: Dict = {"limit": str(limit), "symbol": convert_to_exchange_trading_pair(trading_pair)} if limit != 0 \
            else {"symbol": convert_to_exchange_trading_pair(trading_pair)}
        async with client.get(SNAPSHOT_REST_URL, params=params) as response:
            response: aiohttp.ClientResponse = response
            if response.status != 200:
                raise IOError(f"Error fetching New Capital market snapshot for {trading_pair}. "
                              f"HTTP status is {response.status}.")
            data: Dict[str, Any] = await response.json()

            return data

    async def get_new_order_book(self, trading_pair: str) -> OrderBook:
        async with aiohttp.ClientSession() as client:
            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair, 1000)
            snapshot_timestamp: float = time.time()
            snapshot_msg: OrderBookMessage = NewcapitalOrderBook.snapshot_message_from_exchange(
                snapshot,
                snapshot_timestamp,
                metadata={"trading_pair": trading_pair}
            )
            order_book = self.order_book_create_function()
            order_book.apply_snapshot(snapshot_msg.bids, snapshot_msg.asks, snapshot_msg.update_id)
            return order_book

    async def _inner_messages(self,
                              ws: websockets.WebSocketClientProtocol) -> AsyncIterable[str]:
        try:
            while True:
                try:
                    msg: str = await asyncio.wait_for(ws.recv(), timeout=self.MESSAGE_TIMEOUT)
                    yield msg
                except asyncio.TimeoutError:
                    try:
                        pong_waiter = await ws.ping()
                        await asyncio.wait_for(pong_waiter, timeout=self.PING_TIMEOUT)
                    except asyncio.TimeoutError:
                        raise
        except asyncio.TimeoutError:
            self.logger().warning("WebSocket ping timed out. Going to reconnect...")
            return
        except ConnectionClosed:
            return
        finally:
            await ws.close()

    async def listen_for_trades(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            for trading_pair in self._trading_pairs:
                try:
                    stream_url: str = f"{WS_URL}?pair={convert_to_exchange_trading_pair(trading_pair).upper()}"
                    async with websockets.connect(stream_url) as ws:
                        ws: websockets.WebSocketClientProtocol = ws
                        async for raw_msg in self._inner_messages(ws):
                            msg = ujson.loads(raw_msg)
                            if msg.get("frame", "") == "trade":
                                trade_msg: OrderBookMessage = NewcapitalOrderBook.trade_message_from_exchange(
                                    msg,
                                    metadata={"trading_pair": trading_pair}
                                )
                                output.put_nowait(trade_msg)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                        exc_info=True)
                    await asyncio.sleep(30.0)

    async def listen_for_order_book_diffs(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            for trading_pair in self._trading_pairs:
                try:
                    stream_url: str = f"{WS_URL}?pair={convert_to_exchange_trading_pair(trading_pair).upper()}"
                    async with websockets.connect(stream_url) as ws:
                        ws: websockets.WebSocketClientProtocol = ws
                        async for raw_msg in self._inner_messages(ws):
                            msg = ujson.loads(raw_msg)
                            if msg.get("frame", "") == "orders":
                                order_book_message: OrderBookMessage = NewcapitalOrderBook.diff_message_from_exchange(
                                    msg["data"],
                                    time.time(),
                                    metadata={"trading_pair": trading_pair}
                                )
                                output.put_nowait(order_book_message)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    self.logger().error("Unexpected error with WebSocket connection. Retrying after 30 seconds...",
                                        exc_info=True)
                    await asyncio.sleep(30.0)

    async def listen_for_order_book_snapshots(self, ev_loop: asyncio.BaseEventLoop, output: asyncio.Queue):
        while True:
            try:
                async with aiohttp.ClientSession() as client:
                    for trading_pair in self._trading_pairs:
                        try:
                            snapshot: Dict[str, Any] = await self.get_snapshot(client, trading_pair)
                            snapshot_timestamp: float = time.time()
                            snapshot_msg: OrderBookMessage = NewcapitalOrderBook.snapshot_message_from_exchange(
                                snapshot,
                                snapshot_timestamp,
                                metadata={"trading_pair": trading_pair}
                            )
                            output.put_nowait(snapshot_msg)
                            self.logger().debug(f"Saved order book snapshot for {trading_pair}")
                            await asyncio.sleep(5.0)
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            self.logger().error("Unexpected error.", exc_info=True)
                            await asyncio.sleep(5.0)
                    this_hour: pd.Timestamp = pd.Timestamp.utcnow().replace(minute=0, second=0, microsecond=0)
                    next_hour: pd.Timestamp = this_hour + pd.Timedelta(hours=1)
                    delta: float = next_hour.timestamp() - time.time()
                    await asyncio.sleep(delta)
            except asyncio.CancelledError:
                raise
            except Exception:
                self.logger().error("Unexpected error.", exc_info=True)
                await asyncio.sleep(5.0)
