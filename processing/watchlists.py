import logging
from typing import Dict
from processing.helpers import Context, generic_string_in_set_with_cache

logger = logging.getLogger(__name__)

REDIS_KEY_CACHE_WATCHED_USERS = 'cache-watchlist-users'
REDIS_KEY_CACHE_WATCHED_ORGS = 'cache-watchlist-orgs'


def flag_if_on_watchlist(context: Context, event: Dict):
    event['n8nFlags'] = {}

    username = event.get('initiator_user_name')
    on_users_watchlist = _user_in_watchlist(context, username)
    event['n8nFlags']['onUserWatchlist'] = on_users_watchlist

    org_id = event.get('dataset_obj', {}).get('organization', {}).get('id')
    on_org_watchlist = _org_in_watchlist(context, org_id)
    event['n8nFlags']['onOrgWatchlist'] = on_org_watchlist



def _user_in_watchlist(context: Context, username: str) -> bool:
    return generic_string_in_set_with_cache(context, username, REDIS_KEY_CACHE_WATCHED_USERS,
                                            context.config.SHEET_NAME_WATCHED_USERS,
                                            context.config.COL_INDEX_WATCHED_USERS)

def _org_in_watchlist(context: Context, org_id: str) -> bool:
    return generic_string_in_set_with_cache(context, org_id, REDIS_KEY_CACHE_WATCHED_ORGS,
                                            context.config.SHEET_NAME_WATCHED_ORGS,
                                            context.config.COL_INDEX_WATCHED_ORGS)
