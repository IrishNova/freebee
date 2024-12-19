"""

@rìan

Connects to kalshi orderbook feed.

This is a demo, not for production. 

"""


import asyncio
import websockets
import orjson
import base64
import time
from typing import Dict
from datetime import datetime
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.hazmat.backends import default_backend
from cryptography.exceptions import InvalidSignature


class KalshiAuth:
    """
    New authentication class using API keys.
    Handles signing requests with RSA-PSS signatures.
    """

    def __init__(self, private_key_path: str, key_id: str):
        """
        Initialize the auth handler with API key credentials.

        Args:
            private_key_path: Path to the RSA private key file (PEM format)
            key_id: API key identifier provided by Kalshi
        """
        self.key_id = key_id
        self.private_key = self._load_private_key(private_key_path)

    def _load_private_key(self, file_path: str) -> rsa.RSAPrivateKey:
        """
        Load RSA private key from PEM file.

        Args:
            file_path: Path to the private key file

        Returns:
            RSAPrivateKey: Loaded private key object
        """
        with open(file_path, "rb") as key_file:
            return serialization.load_pem_private_key(
                key_file.read(), password=None, backend=default_backend()
            )

    def _sign_message(self, message: str) -> str:
        """
        Sign a message using RSA-PSS.

        Args:
            message: Message to sign

        Returns:
            str: Base64-encoded signature
        """
        try:
            signature = self.private_key.sign(
                message.encode("utf-8"),
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.DIGEST_LENGTH,
                ),
                hashes.SHA256(),
            )
            return base64.b64encode(signature).decode("utf-8")
        except InvalidSignature as e:
            raise ValueError("Failed to sign message") from e

    def get_auth_headers(self, method: str, path: str) -> Dict[str, str]:
        """
        Generate authentication headers for an API request.

        Args:
            method: HTTP method (GET, POST, etc.)
            path: API endpoint path

        Returns:
            Dict[str, str]: Headers to include with the API request
        """
        timestamp = str(int(time.time() * 1000))
        message = timestamp + method + path

        return {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": self._sign_message(message),
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "Content-Type": "application/json",
        }


class KalshiMonitor:
    def __init__(self):
        self.api_url = "https://api.elections.kalshi.com"
        self.ws_url = "wss://api.elections.kalshi.com/trade-api/ws/v2"
        self.auth_handler = KalshiAuth(
            private_key_path=".pem",
            key_id=""
        )
        self.websocket = None
        self.command_id = 0
        self.orderbook = {"yes": {}, "no": {}}

    async def connect(self):
        """Connect to websocket with API key authentication"""
        headers = self.auth_handler.get_auth_headers("GET", "/trade-api/ws/v2")
        print("Connecting with headers:", headers)
        self.websocket = await websockets.connect(
            self.ws_url,
            extra_headers=headers,
            ping_interval=None,
            ping_timeout=30
        )
        print("WebSocket connected successfully")

    def print_orderbook(self):
        print("\n" + "=" * 50)
        print(f"Orderbook State at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
        print("=" * 50)

        print("\nYES Side:")
        if self.orderbook["yes"]:
            for price, quantity in sorted(self.orderbook["yes"].items(), reverse=True):
                print(f"  Price: {price:3d} | Quantity: {quantity:5d}")
        else:
            print("  No orders")

        print("\nNO Side:")
        if self.orderbook["no"]:
            for price, quantity in sorted(self.orderbook["no"].items(), reverse=True):
                print(f"  Price: {price:3d} | Quantity: {quantity:5d}")
        else:
            print("  No orders")
        print("=" * 50 + "\n")

    async def handle_snapshot(self, msg):
        print(f"\nReceived Orderbook Snapshot at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")

        # Reset current orderbook
        self.orderbook = {"yes": {}, "no": {}}

        # Update with snapshot data
        if "yes" in msg:
            for price_level in msg["yes"]:
                self.orderbook["yes"][price_level[0]] = price_level[1]

        if "no" in msg:
            for price_level in msg["no"]:
                self.orderbook["no"][price_level[0]] = price_level[1]

        self.print_orderbook()

    async def handle_delta(self, msg):
        print(f"\nReceived Delta at {datetime.now().strftime('%H:%M:%S.%f')[:-3]}")
        print(f"Side: {msg['side']}, Price: {msg['price']}, Delta: {msg['delta']}")

        # Update orderbook
        if msg["delta"] == 0:
            self.orderbook[msg["side"]].pop(msg["price"], None)
        else:
            self.orderbook[msg["side"]][msg["price"]] = msg["delta"]

        self.print_orderbook()

    async def subscribe_to_orderbook(self, ticker):
        self.command_id += 1
        subscribe_message = {
            "id": self.command_id,
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta"],
                "market_tickers": [ticker]
            }
        }
        print("\nSending subscription message:")
        print(orjson.dumps(subscribe_message).decode())
        await self.websocket.send(orjson.dumps(subscribe_message))
        print(f"Subscribed to orderbook for {ticker}")

    async def monitor(self, ticker):
        while True:
            try:
                print(f"\nStarting monitor for {ticker}...")
                headers = self.auth_handler.get_auth_headers("GET", "/trade-api/ws/v2")
                print("\nConnecting with headers:")
                print(headers)

                self.websocket = await websockets.connect(
                    self.ws_url,
                    extra_headers=headers,
                    ping_interval=None,
                    ping_timeout=30
                )
                print("WebSocket connected successfully")

                await self.subscribe_to_orderbook(ticker)
                print("Waiting for messages...")

                while True:
                    message = await self.websocket.recv()
                    print("\nRAW MESSAGE RECEIVED:")
                    if isinstance(message, bytes):
                        message = message.decode('utf-8')
                    print(message)

                    if message == 'heartbeat':
                        print("(Heartbeat received)")
                        continue

                    try:
                        data = orjson.loads(message)
                        print("\nPARSED JSON:")
                        print(orjson.dumps(data, option=orjson.OPT_INDENT_2).decode())

                        msg_type = data.get('type')
                        if msg_type == 'error':
                            print(f"Error received: {data}")
                        elif msg_type == 'orderbook_snapshot':
                            print("\nProcessing orderbook snapshot...")
                            await self.handle_snapshot(data['msg'])
                        elif msg_type == 'orderbook_delta':
                            print("\nProcessing orderbook delta...")
                            await self.handle_delta(data['msg'])
                        elif msg_type == 'subscribed':
                            print(f"\nReceived subscription confirmation: {data}")
                        else:
                            print(f"\nUnhandled message type: {msg_type}")
                    except Exception as e:
                        print(f"Error processing message: {str(e)}")
                        print(f"Problem message was: {message}")

            except websockets.ConnectionClosed:
                print("WebSocket connection closed, reconnecting...")
                await asyncio.sleep(5)
            except Exception as e:
                print(f"Error occurred: {str(e)}")
                await asyncio.sleep(5)



async def main():
    monitor = KalshiMonitor()
    ticker = "KXBTCD-24DEC2017-T101999.99"
    await monitor.monitor(ticker)


if __name__ == "__main__":
    asyncio.run(main())
