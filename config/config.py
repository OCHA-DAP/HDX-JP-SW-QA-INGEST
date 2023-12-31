from dataclasses import dataclass

import logging
import os
import gspread
import gspread.client as gs_client
import slack_sdk
import slack_sdk.errors as slack_errors


logger = logging.getLogger(__name__)

@dataclass
class Config:
    # pylint: disable=invalid-name
    WORKER_ENABLED: bool
    GOOGLE_SHEETS_PRIVATE_KEY: str
    GOOGLE_SHEETS_CLIENT_EMAIL: str
    GOOGLE_SHEETS_TOKEN_URI: str

    SPREADSHEET_NAME: str

    SHEET_NAME_LIMIT_BATCHES: str
    COL_NAME_LIMIT_BATCHES: str
    DURATION_MINUTES_LIMIT_BATCHES: int
    MAX_ENTRIES_LIMIT_BATCHES: int

    SHEET_NAME_FILTERED_USERS: str
    COL_INDEX_FILTERED_USERS: int
    SHEET_NAME_FILTERED_ORGS: str
    COL_INDEX_FILTERED_ORGS: int
    SHEET_NAME_FILTERED_DATASETS: str
    COL_INDEX_FILTERED_DATASETS: int

    SHEET_NAME_WATCHED_USERS: str
    COL_INDEX_WATCHED_USERS: int
    SHEET_NAME_WATCHED_ORGS: str
    COL_INDEX_WATCHED_ORGS: int

CONFIG = None

def get_config() -> Config:
    global CONFIG
    if not CONFIG:
        CONFIG = Config(
            WORKER_ENABLED=os.getenv('WORKER_ENABLED')=='true',
            GOOGLE_SHEETS_PRIVATE_KEY=os.getenv('GOOGLE_SHEETS_PRIVATE_KEY'),
            GOOGLE_SHEETS_CLIENT_EMAIL=os.getenv('GOOGLE_SHEETS_CLIENT_EMAIL'),
            GOOGLE_SHEETS_TOKEN_URI=os.getenv('GOOGLE_SHEETS_TOKEN_URI'),

            SPREADSHEET_NAME=os.getenv('SPREADSHEET_NAME', 'Local QA Safelist test'),

            SHEET_NAME_LIMIT_BATCHES=os.getenv('SHEET_NAME_LIMIT_BATCHES', 'Too Many Datasets'),
            COL_NAME_LIMIT_BATCHES=os.getenv('COL_NAME_LIMIT_BATCHES', 'id'),
            DURATION_MINUTES_LIMIT_BATCHES=int(os.getenv('DURATION_MINUTES_LIMIT_BATCHES', '60')),
            MAX_ENTRIES_LIMIT_BATCHES=int(os.getenv('MAX_ENTRIES_LIMIT_BATCHES', '7')),

            SHEET_NAME_FILTERED_USERS=os.getenv('SHEET_NAME_FILTERED_USERS', 'User Safelist'),
            COL_INDEX_FILTERED_USERS=int(os.getenv('COL_INDEX_FILTERED_USERS', '2')),
            SHEET_NAME_FILTERED_ORGS=os.getenv('SHEET_NAME_FILTERED_ORGS', 'Organization Safelist'),
            COL_INDEX_FILTERED_ORGS=int(os.getenv('COL_INDEX_FILTERED_ORGS', '1')),
            SHEET_NAME_FILTERED_DATASETS=os.getenv('SHEET_NAME_FILTERED_DATASETS', 'Dataset Safelist'),
            COL_INDEX_FILTERED_DATASETS=int(os.getenv('COL_INDEX_FILTERED_DATASETS', '1')),

            SHEET_NAME_WATCHED_USERS=os.getenv('SHEET_NAME_WATCHED_USERS', 'User Watchlist'),
            COL_INDEX_WATCHED_USERS=int(os.getenv('COL_INDEX_WATCHED_USERS', '2')),
            SHEET_NAME_WATCHED_ORGS=os.getenv('SHEET_NAME_WATCHED_ORGS', 'Organization Watchlist'),
            COL_INDEX_WATCHED_ORGS=int(os.getenv('COL_INDEX_WATCHED_ORGS', '1')),
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


class SlackClientWrapper():
    def __init__(self) -> None:
        self.slack_channel = os.getenv('HDX_N8N_SLACK_NOTIFICATION_CHANNEL')
        # self.slack_channel = 'test-channel'


        self.slack_client = None
        token = os.getenv('HDX_N8N_SLACK_CENTRE_ACCESS_TOKEN')
        if token:
            self.slack_client = slack_sdk.WebClient(token=token)
            logger.debug('Slack client initialized')

    def post_to_slack_channel(self, message: str):
        if self.slack_client:
            try:
                text = f'[QA INGEST] {message}'
                response = self.slack_client.chat_postMessage(channel=self.slack_channel, text=text)
            except slack_errors.SlackApiError as e:
                # You will get a SlackApiError if "ok" is False
                # assert e.response["ok"] is False
                logger.error(f"Got an error: {e.response['error']}")
        else:
            logger.info(f'[instead of slack] {message}')
