import logging

from dataclasses import dataclass
from hdx_redis_lib import RedisKeyValueStore
from gspread.client import Client
from config.config import Config, SlackClientWrapper
from processing.fetching import fetch_values

logger = logging.getLogger(__name__)


@dataclass
class Context:
    store: RedisKeyValueStore
    config: Config
    gsheets: Client
    slack_client: SlackClientWrapper


def generic_string_in_set_with_cache(context:Context, search_string: str, cache_key: str, worksheet_name:str,
                                     worksheet_col:int, expire_in_seconds: int=None) -> bool:
    """Checks whether a string is in a Google Spreadsheet column (while caching the column in redis as a set).
    :param context:
    :param search_string:
    :param cache_key: Redis key that will be used.
    :param worksheet_name: The name of the sheet inside the spreadsheet
    :param worksheet_col: The index of the column ( starts at 1 !!! )
    :param expire_in_seconds: Expiration time in seconds
    :return: True if search_string is in the worksheet column. False otherwise
    """
    found_in_cache = False
    if search_string:
        found_in_cache = context.store.is_in_set(cache_key, search_string)

        # found_in_cache might be false simply because the cache does not exist
        cache_exist = context.store.exists(cache_key)
        if not cache_exist:
            values = fetch_values(context.gsheets, context.config.SPREADSHEET_NAME, worksheet_name, worksheet_col)
            if values:
                context.store.set_set(cache_key, values, expire_in_seconds)
                found_in_cache = context.store.is_in_set(cache_key, search_string)
            else:
                logger.error('List for {} from google spreadsheet is empty'.format(cache_key))
                found_in_cache = False
    else:
        logger.warning('Search string should NOT be empty for {}'.format(cache_key))

    return found_in_cache
