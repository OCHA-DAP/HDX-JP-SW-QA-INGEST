from dataclasses import dataclass

import logging
import os
import gspread
import gspread.client as gs_client


logger = logging.getLogger(__name__)

@dataclass
class Config:
    # pylint: disable=invalid-name
    GOOGLE_SHEETS_PRIVATE_KEY: str
    GOOGLE_SHEETS_CLIENT_EMAIL: str
    GOOGLE_SHEETS_TOKEN_URI: str

    SPREADSHEET_NAME: str

    SHEET_NAME_FILTERED_USERS: str
    COL_INDEX_FILTERED_USERS: int

    SHEET_NAME_WATCHED_USERS: str
    COL_INDEX_WATCHED_USERS: int
    SHEET_NAME_WATCHED_ORGS: str
    COL_INDEX_WATCHED_ORGS: int

    HDX_ENVIRONMENT: str

CONFIG = None

def get_config() -> Config:
    global CONFIG
    if not CONFIG:
        CONFIG = Config(
            GOOGLE_SHEETS_PRIVATE_KEY=os.getenv('GOOGLE_SHEETS_PRIVATE_KEY'),
            GOOGLE_SHEETS_CLIENT_EMAIL=os.getenv('GOOGLE_SHEETS_CLIENT_EMAIL'),
            GOOGLE_SHEETS_TOKEN_URI=os.getenv('GOOGLE_SHEETS_TOKEN_URI'),

            SPREADSHEET_NAME=os.getenv('SPREADSHEET_NAME', 'Local QA Safelist test'),

            SHEET_NAME_FILTERED_USERS=os.getenv('SHEET_NAME_FILTERED_USERS', 'User Safelist'),
            COL_INDEX_FILTERED_USERS=int(os.getenv('COL_INDEX_FILTERED_USERS', '2')),

            SHEET_NAME_WATCHED_USERS=os.getenv('SHEET_NAME_WATCHED_USERS', 'User Watchlist'),
            COL_INDEX_WATCHED_USERS=int(os.getenv('COL_INDEX_WATCHED_USERS', '2')),
            SHEET_NAME_WATCHED_ORGS=os.getenv('SHEET_NAME_WATCHED_USERS', 'Organization Watchlist'),
            COL_INDEX_WATCHED_ORGS=int(os.getenv('COL_INDEX_WATCHED_ORGS', '1')),

            HDX_ENVIRONMENT=os.getenv('HDX_ENVIRONMENT', 'local')
        )

    return CONFIG

def get_gsheetes() -> gs_client.Client:
    if not CONFIG:
        get_config()
    try:
        gsheet = gspread.service_account_from_dict({
            "private_key": CONFIG.GOOGLE_SHEETS_PRIVATE_KEY,
            "client_email": CONFIG.GOOGLE_SHEETS_CLIENT_EMAIL,
            "token_uri": CONFIG.GOOGLE_SHEETS_TOKEN_URI,
        })
        return gsheet
    except Exception as exc:
        logger.error(f'Exception of type {type(exc).__name__} while creating google sheets client: {str(exc)}')
        raise
