import logging
from gspread.client import Client

logger = logging.getLogger(__name__)


def fetch_values(gsheets: Client, spreadsheet_name: str, worksheet_name: str, worksheet_col: int):
    """Fetch values from the configured Google Spreadsheet

    :param gsheets:
    :param spreadsheet_name: The name of the spreadsheet
    :param worksheet_name: The name of the sheet inside the spreadsheet
    :param worksheet_col: The index of the column ( starts at 1 !!! )
    :return: List of value from the column without the 1st row (the header row)
    """

    try:
        spreadsheet = gsheets.open(spreadsheet_name)
        worksheet = spreadsheet.worksheet(worksheet_name)
        column_values = worksheet.col_values(worksheet_col)
        if column_values and len(column_values) > 1:
            # we're removing the header of the column
            return column_values[1:]
        else:
            return None

    except Exception as exc:
        logger.error(f'Exception of type {type(exc).__name__} while fetching values from spreadsheet: {str(exc)}')
        raise