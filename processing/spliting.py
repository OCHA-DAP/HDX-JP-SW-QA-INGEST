import logging
from typing import Dict
from processing.helpers import Context

logger = logging.getLogger(__name__)


def split_each_field_own_event(context: Context, event: Dict) -> list:
    # split each dataset metadata field change into its own event
    events = []
    event_type = event['event_type']
    changed_fields = event.get('changed_fields', [])
    event['changed_fields'] = changed_fields
    if  event_type == 'dataset-metadata-changed' or event_type == 'dataset-created':
        for changed_field in event['changed_fields']:
            new_event = event.copy()
            new_event['changed_fields'] = [changed_field]
            events.append(new_event)
    elif event_type == 'resource-created' or event_type == 'resource-data-changed':
        # add the original event
        events.append(event)
        # generate the SDD duplicated event
        sdd_event = event.copy()
        sdd_event['event_type'] = f'sdd-{event_type}'
        events.append(sdd_event)
    else:
        events.append(event)
    return events
