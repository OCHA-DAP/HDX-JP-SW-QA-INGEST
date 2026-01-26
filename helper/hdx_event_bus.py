import os
import logging
from typing import Any, Dict, Iterable

from hdx_redis_lib import connect_to_hdx_write_only_event_bus, RedisConfig

event_bus = None
logger = logging.getLogger(__name__)


def _get_event_bus():
    global event_bus
    if event_bus is None:
        redis_stream_host = os.getenv('REDIS_STREAM_HOST', 'redis')
        redis_stream_port = int(os.getenv('REDIS_STREAM_PORT', '6379'))
        redis_stream_db = int(os.getenv('REDIS_STREAM_DB', '7'))
        redis_stream_name = os.getenv('REDIS_STREAM_STREAM_NAME', 'hdx_event_stream')

        event_bus = connect_to_hdx_write_only_event_bus(
            redis_stream_name,
            RedisConfig(host=redis_stream_host, port=redis_stream_port, db=redis_stream_db),
        )

    return event_bus


def stream_events_to_redis(event_list: Iterable[Dict[str, Any]]) -> None:
    event_bus = _get_event_bus()

    for event in event_list:
        event_type = event.get('event_type')
        try:
            logger.info('Pushing event type %s to redis stream', event_type)
            event_bus.push_hdx_event(dict(event))
        except Exception:
            logger.exception('Failed pushing event type %s to redis stream', event_type)
