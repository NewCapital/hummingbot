import re
from typing import (
    Optional,
    Tuple)

from hummingbot.client.config.config_var import ConfigVar
from hummingbot.client.config.config_methods import using_exchange


CENTRALIZED = True


EXAMPLE_PAIR = "TWINS-BTC"


DEFAULT_FEES = [0.1, 0.1]


def convert_from_exchange_trading_pair(exchange_trading_pair: str) -> Optional[str]:
    return exchange_trading_pair.replace("_", "-")


def convert_to_exchange_trading_pair(hb_trading_pair: str) -> str:
    return hb_trading_pair.replace("-", "_")


KEYS = {
    "newcapital_api_key":
        ConfigVar(key="newcapital_api_key",
                  prompt="Enter your New Capital API key >>> ",
                  required_if=using_exchange("newcapital"),
                  is_secure=True,
                  is_connect_key=True),
    "newcapital_api_secret":
        ConfigVar(key="newcapital_api_secret",
                  prompt="Enter your New Capital API secret >>> ",
                  required_if=using_exchange("newcapital"),
                  is_secure=True,
                  is_connect_key=True),
}
