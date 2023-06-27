import logging
from typing import Dict
from processing.helpers import Context
from processing.limit_batches import limit
from processing.filtering import filter_out
from processing.populating import populate_changed_fields, populate_with_redis_key
from processing.spliting import split_each_field_own_event

logger = logging.getLogger(__name__)


def process(context: Context, event: Dict):

    filtered_out = filter_out(context, event)
    if filtered_out:
        logger.info(f'[safelist] Discarded event of type {event.get("event_type")} for dataset {event.get("dataset_name")}')
        return

    should_limit = limit(context, event)
    if should_limit:
        logger.info(f'[batch-limiting] Discarded event of type {event.get("event_type")} for dataset {event.get("dataset_name")}')
        return

    populate_changed_fields(event)

    events = split_each_field_own_event(context, event)

    for event in events:
        populate_with_redis_key(context.config, event)
        context.store.set_object(event['redisKey'], event)
        logger.info(f'Dataset name is {event.get("dataset_name")}')
