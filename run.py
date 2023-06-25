import logging
import json

from hdx_redis_lib import connect_to_hdx_event_bus_with_env_vars

logger = logging.getLogger(__name__)


def event_processor(event):
            logger.info('Received event: ' + json.dumps(event, ensure_ascii=False, indent=4))
            dataset_id = event.get('dataset_id')
            
            return True, 'Success'

if __name__ == "__main__":
     # Connect to Redis
    event_bus = connect_to_hdx_event_bus_with_env_vars()

    event_bus.hdx_listen(event_processor)