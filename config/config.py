from typing import Dict
import os


CONFIG = {}


def get_config() -> Dict:
    if not CONFIG:
        CONFIG['GOOGLE_SHEETS_PRIVATE_KEY'] = os.getenv('GOOGLE_SHEETS_PRIVATE_KEY')
        CONFIG['GOOGLE_SHEETS_CLIENT_EMAIL'] = os.getenv('GOOGLE_SHEETS_CLIENT_EMAIL')
        CONFIG['GOOGLE_SHEETS_TOKEN_URI'] = os.getenv('GOOGLE_SHEETS_TOKEN_URI')
    return CONFIG
