from dataclasses import dataclass
from typing import Dict
from hdx_redis_lib import RedisKeyValueStore
from gspread.client import Client


@dataclass
class Context:
    store: RedisKeyValueStore
    config: Dict
    gsheets: Client
