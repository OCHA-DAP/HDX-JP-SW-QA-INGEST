import logging
from typing import Any, Dict

from helper.hdx_event_bus import stream_events_to_redis

logger = logging.getLogger(__name__)

_SDD_EVENT_TYPES = {'resource-created', 'resource-data-changed'}


def send_sdd_event(event: Dict[str, Any]) -> None:
    """
    Duplicate selected resource events to the SDD event bus (Redis stream).
    """
    event_type = event.get('event_type')
    if event_type not in _SDD_EVENT_TYPES:
        return

    sdd_event = dict(event)
    sdd_event['event_type'] = f'sdd-{event_type}'

    logger.info('Publishing SDD duplicate event type %s', sdd_event['event_type'])
    stream_events_to_redis([sdd_event])
