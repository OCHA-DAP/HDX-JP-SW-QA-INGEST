import logging
import logging.config
logging.config.fileConfig('logging.conf')
logger = logging.getLogger(__name__)


import datetime
import json

from hdx_redis_lib import connect_to_hdx_event_bus_with_env_vars, connect_to_key_value_store_with_env_vars
from processing.main import process
from processing.helpers import Context
from config.config import get_config, get_gsheetes, SlackClientWrapper
from helper.util import do_nothing_for_ever


ALLOWED_EVENT_TYPES = {
    'dataset-created',
    'dataset-deleted',
    'dataset-metadata-changed',

    'resource-deleted',
    'resource-created',
    'resource-data-changed',
    'resource-metadata-changed',

    'spreadsheet-sheet-created',
    'spreadsheet-sheet-deleted',
    'spreadsheet-sheet-changed',
}

config = get_config()

if __name__ == "__main__":
    if not config.WORKER_ENABLED:
        do_nothing_for_ever()
        key_value_store = None
        gc = None

    else:
        key_value_store = connect_to_key_value_store_with_env_vars(expire_in_seconds=60*60*12)
        gc = get_gsheetes()


        def event_processor(event):
            logger.info('Received event: ' + json.dumps(event, ensure_ascii=False, indent=4))
            start_time = datetime.datetime.now()

            context = Context(store=key_value_store, config=config, gsheets=gc, slack_client=SlackClientWrapper())
            process(context, event)
            end_time = datetime.datetime.now()
            elapsed_time = end_time - start_time
            logger.info(f'Finished processing event '
                        f'of type {event["event_type"]} from {event["event_time"]} in {str(elapsed_time)}')

            return True, 'Success'

        # Connect to Redis
        event_bus = connect_to_hdx_event_bus_with_env_vars()
        logger.info('Connected to Redis')

        event_bus.hdx_listen(event_processor, allowed_event_types=ALLOWED_EVENT_TYPES, max_iterations=10_000)
