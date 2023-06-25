from dataclasses import dataclass
from typing import Dict
from hdx_redis_lib import RedisKeyValueStore


@dataclass
class Context:
    store: RedisKeyValueStore
    config: Dict
