from typing import Dict
from gspread.client import Client

USERS_SHEET_NAME = 'User Safelist'


def fetch_users(gc: Client, config: Dict):
    spread_sheet = gc.open('Local QA Safelist test')
    user_sheet = spread_sheet.worksheet(USERS_SHEET_NAME)
    user_id_column = user_sheet.col_values(1)
    if user_id_column and len(user_id_column) > 1:
        # we're removing the header of the column
        return user_id_column[1:]
    else:
        return None
