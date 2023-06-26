import logging
from typing import Dict
from processing.helpers import Context

logger = logging.getLogger(__name__)


def populate_changed_fields(context: Context, event: Dict) -> dict:
    # create changed fields for dataset/resource deleted
    event_type = event['event_type']
    if event_type == 'dataset-deleted' or event_type == 'resource-deleted':
        event['changed_fields'] = [{
            'field': event_type,
            'new_value': 'deleted',
            'old_value': None
        }]
    return event
