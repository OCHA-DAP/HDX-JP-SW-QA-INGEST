import logging
import logging.config
logging.config.fileConfig('logging.conf')
logger = logging.getLogger(__name__)


import json
import gspread

from hdx_redis_lib import connect_to_hdx_event_bus_with_env_vars, connect_to_key_value_store_with_env_vars
from processing.main import process
from processing.helpers import Context
from config.config import get_config


key_value_store = connect_to_key_value_store_with_env_vars(expire_in_seconds=60*30)
config = get_config()
gc = gspread.service_account_from_dict({
    "private_key": config['GOOGLE_SHEETS_PRIVATE_KEY'],
    "client_email": config['GOOGLE_SHEETS_CLIENT_EMAIL'],
    "token_uri": config['GOOGLE_SHEETS_TOKEN_URI'],
})


def event_processor(event):
    logger.info('Received event: ' + json.dumps(event, ensure_ascii=False, indent=4))

    context = Context(store=key_value_store, config=config, gsheets=gc)
    process(context, event)

    return True, 'Success'

if __name__ == "__main__":
     # Connect to Redis
    event_bus = connect_to_hdx_event_bus_with_env_vars()
    logger.info('Connected to Redis')

    event_bus.hdx_listen(event_processor)
