import logging
from typing import Dict
from processing.helpers import Context

logger = logging.getLogger(__name__)


def split_each_field_own_event(context: Context, event: Dict) -> list:
    # split each dataset metadata field change into its own event
    events = []
    event_type = event['event_type']
    changed_fields = event['changed_fields']
    event['changed_fields'] = changed_fields if changed_fields else []
    if  event_type == 'dataset-metadata-changed' or event_type == 'dataset-created':
        for changed_field in event['changed_fields']:
            new_event = event.copy()
            new_event['changed_fields'] = [changed_field]
            events.append(new_event)
    else:
        events.append(event)
    return events
