import logging
from typing import Dict
from processing.helpers import Context, generic_string_in_set_with_cache

logger = logging.getLogger(__name__)

REDIS_KEY_CACHE_FILTERED_USERS = 'cache-filtered-users'

def filter_out(context: Context, event: Dict) -> bool:
    username = event.get('initiator_user_name')
    filtered_out = _filter_out_users(context, username)
    return filtered_out


def _filter_out_users(context: Context, username: str) -> bool:
    return generic_string_in_set_with_cache(context, username, REDIS_KEY_CACHE_FILTERED_USERS,
                                            context.config.SHEET_NAME_FILTERED_USERS,
                                            context.config.COL_INDEX_FILTERED_USERS)


