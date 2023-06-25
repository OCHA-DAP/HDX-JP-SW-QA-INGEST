from typing import Dict
import gspread

USERS_SHEET_NAME = 'User Safelist'


def fetch_users(config: Dict):
    gc = gspread.service_account_from_dict({
        "private_key": config['GOOGLE_SHEETS_PRIVATE_KEY'],
        "client_email": config['GOOGLE_SHEETS_CLIENT_EMAIL'],
        "token_uri": config['GOOGLE_SHEETS_TOKEN_URI'],
    })
    spread_sheet = gc.open('Local QA Safelist test')
    user_sheet = spread_sheet.worksheet(USERS_SHEET_NAME)
    user_id_column = user_sheet.col_values(1)
    if user_id_column and len(user_id_column) > 1:
        # we're removing the header of the column
        return user_id_column[1:]
    else:
        return None