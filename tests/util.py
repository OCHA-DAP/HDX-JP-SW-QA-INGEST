from gspread.client import Client


def fetch_values_wrapper(gsheets: Client, spreadsheet_name: str, worksheet_name: str, worksheet_col: int):
    if worksheet_name == 'User Safelist':
        return [
            'blocked-test-user'
        ]
    elif worksheet_name == 'Dataset Safelist':
        return [
            'blocked-test-dataset-id'
        ]
    elif worksheet_name == 'Organization Safelist':
        return [
            'blocked-test-org-id'
        ]
    elif worksheet_name == 'User Watchlist':
        return [
            'watchlisted-test-user'
        ]
    elif worksheet_name == 'Organization Watchlist':
        return [
            'watchlisted-test-org-id'
        ]
