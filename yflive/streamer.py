from typing import List
from websockets import connect, ConnectionClosed

import json

from ._reader import _QuoteReader

YAHOO_FINANCE_SOCKET = "wss://streamer.finance.yahoo.com/"

class QuoteStreamer():

    def __init__(self, uri):
        self.uri = uri
        self.ws = None
        self.tickers : List[str] = []
        self._connected = False

    async def subscribe(self, tickers: List[str]) -> None:
        self.tickers = tickers
        if self._connected:
            await self._subscribe()

    async def _subscribe(self) -> None:
        msg = json.dumps({"subscribe": self.tickers})
        await self.ws.send(msg)

    async def run(self):
        async for websocket in connect(self.uri):
            self._connected = True
            self.ws = websocket

            try:
                await self._subscribe()
                async for message in websocket:
                    quote = _QuoteReader.parse(message)
                    yield quote
            except ConnectionClosed:
                self._connected = False
                continue            
            except Exception as e:
                print(e)