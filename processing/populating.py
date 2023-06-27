import logging
from typing import Dict

logger = logging.getLogger(__name__)


def populate_changed_fields(event: Dict) -> Dict:
    # create changed fields for dataset/resource deleted
    event_type = event['event_type']
    if event_type == 'dataset-deleted' or event_type == 'resource-deleted':
        event['changed_fields'] = [{
            'field': event_type,
            'new_value': 'deleted',
            'old_value': None
        }]
    return event

def populate_with_redis_key(config: Dict, event: Dict) -> Dict:
    hdx_environment = config['HDX_ENVIRONMENT']
    dataset_name = event.get('dataset_name')
    event_time = event['event_time']
    event_type = event['event_type']
    event_source = event['event_source']
    redis_key = f'ckan-{hdx_environment}-{dataset_name}-{event_time}-{event_type}-{event_source}'
    if event.get('changed_fields'):
        changed_field = event['changed_fields'][0]['field']
        redis_key += f'-{changed_field}'
    event['redisKey'] = redis_key
    return event
