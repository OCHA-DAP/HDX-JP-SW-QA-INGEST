import logging
from typing import Dict
from processing.helpers import Context
from processing.limit_batches import limit
from processing.filtering import filter_out
from processing.populating import populate_changed_fields
from processing.spliting import split_each_field_own_event

logger = logging.getLogger(__name__)


def process(context: Context, event: Dict):

    should_limit = limit(context, event)
    if should_limit:
        logging.info(f'[batch-limiting] Discarded event of type {event.get("event_type")} for dataset {event.get("dataset_name")}')
        return

    filtered_out = filter_out(context, event)
    if filtered_out:
        logging.info(f'[safelist] Discarded event of type {event.get("event_type")} for dataset {event.get("dataset_name")}')
        return

    populate_changed_fields(context, event)

    events = split_each_field_own_event(context, event)

    for event in events:
        logger.info(f'Dataset name is {event.get("dataset_name")}')
