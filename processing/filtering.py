import logging
from typing import Dict
from processing.helpers import Context, generic_string_in_set_with_cache

logger = logging.getLogger(__name__)

REDIS_KEY_CACHE_FILTERED_USERS = 'cache-filtered-users'
REDIS_KEY_CACHE_FILTERED_ORGS = 'cache-filtered-orgs'
REDIS_KEY_CACHE_FILTERED_DATASETS = 'cache-filtered-datasets'

def filter_out(context: Context, event: Dict) -> bool:
    username = event.get('initiator_user_name')
    filtered_out = _filter_out_users(context, username)
    if not filtered_out:
        org_id = event.get('org_id')
        filtered_out = _filter_out_orgs(context, org_id)
    if not filtered_out:
        dataset_id = event['dataset_id']
        filtered_out = _filter_out_datasets(context, dataset_id)

    return filtered_out



def _filter_out_users(context: Context, username: str) -> bool:
    return generic_string_in_set_with_cache(context, username, REDIS_KEY_CACHE_FILTERED_USERS,
                                            context.config.SHEET_NAME_FILTERED_USERS,
                                            context.config.COL_INDEX_FILTERED_USERS)


def _filter_out_orgs(context: Context, org_id: str) -> bool:
    return generic_string_in_set_with_cache(context, org_id, REDIS_KEY_CACHE_FILTERED_ORGS,
                                            context.config.SHEET_NAME_FILTERED_ORGS,
                                            context.config.COL_INDEX_FILTERED_ORGS)

def _filter_out_datasets(context: Context, dataset_id: str) -> bool:
    return generic_string_in_set_with_cache(context, dataset_id, REDIS_KEY_CACHE_FILTERED_DATASETS,
                                            context.config.SHEET_NAME_FILTERED_DATASETS,
                                            context.config.COL_INDEX_FILTERED_DATASETS)