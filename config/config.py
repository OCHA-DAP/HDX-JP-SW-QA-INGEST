import logging
import os
import gspread
import gspread.client as gs_client

from typing import Dict


logger = logging.getLogger(__name__)

CONFIG = {}


def get_config() -> Dict:
    if not CONFIG:
        CONFIG['GOOGLE_SHEETS_PRIVATE_KEY'] = os.getenv('GOOGLE_SHEETS_PRIVATE_KEY')
        CONFIG['GOOGLE_SHEETS_CLIENT_EMAIL'] = os.getenv('GOOGLE_SHEETS_CLIENT_EMAIL')
        CONFIG['GOOGLE_SHEETS_TOKEN_URI'] = os.getenv('GOOGLE_SHEETS_TOKEN_URI')

        CONFIG['HDX_ENVIRONMENT'] = os.getenv('HDX_ENVIRONMENT', 'local')
    return CONFIG

def get_gsheetes() -> gs_client.Client:
    if not CONFIG:
        get_config()
    try:
        gc = gspread.service_account_from_dict({
            "private_key": CONFIG['GOOGLE_SHEETS_PRIVATE_KEY'],
            "client_email": CONFIG['GOOGLE_SHEETS_CLIENT_EMAIL'],
            "token_uri": CONFIG['GOOGLE_SHEETS_TOKEN_URI'],
        })
        return gc
    except Exception as e:
        logger.error(f'Exception of type {type(e).__name__} while creating google sheets client: {str(e)}')
        raise
