import logging
from typing import Dict
from processing.helpers import Context
from processing.fetching import fetch_users

logger = logging.getLogger(__name__)

USER_SET_KEY = 'filtered-users-set'

def filter_out(context: Context, event: Dict) -> bool:
    maintainer_id = event.get('dataset_obj', {}).get('maintainer')
    filtered_out = _filter_out_users(context, maintainer_id)
    return filtered_out


def _filter_out_users(context: Context, user_id: str):
    if user_id:
        if not context.store.exists(USER_SET_KEY):
            user_ids = fetch_users(context.gsheets, context.config)
            if user_ids:
                context.store.set_set(USER_SET_KEY, user_ids)
            else:
                logger.error('User list from google spreadsheet is empty')
        if context.store.is_in_set(USER_SET_KEY, user_id):
            return True
    return False

