import logging
from typing import Dict
from processing.helpers import Context
from gspread.client import Client
from gspread.exceptions import APIError, WorksheetNotFound

logger = logging.getLogger(__name__)


def limit(context: Context, event: Dict) -> bool:
    spreadsheet_id = '1DE1alsrR2lgi3H95WXryyr3kPoMmeYHqMEG39rWLGT0'
    sheet_name = 'Too Many Datasets'
    pk_column = 'id'

    row_data = {
        'id': 'c17eeb2a-9306-4260-b2b6-6ccaeddd231a',
        'name': 'name2',
        'package_creator': 'pc2',
        'updated_by_script': 'ubs2',
        'owner_org': 'owner_org2',
        'organization_name': 'org_name2',
        'title': 'title2',
        'data_update_frequency': 'duq2',
        'metadata_created': 'metadata_c2',
        'metadata_modified': 'metadata_m2',
        'dataset_date': 'dataset_d2',
        'event_type': 'etype2',
        'event_haha': 'busted2',
        'event_time': 'etime2',
        'extra_column': 'etime2'
    }

    update_gsheet(context.gsheets, spreadsheet_id, sheet_name, pk_column, row_data)

    return False


def update_gsheet(gc: Client, spreadsheet_id: str, sheet_name: str, pk_column: str, row_data: Dict) -> bool:
    try:
        sh = gc.open_by_key(spreadsheet_id)
    except APIError:
        logger.info('Error! Invalid spreadsheet ID ({}).'.format(spreadsheet_id))
        return False

    try:
        worksheet = sh.worksheet(sheet_name)
    except WorksheetNotFound:
        logger.info('Error! Worksheet ({}) not found.'.format(sheet_name))
        return False

    if pk_column not in row_data.keys():
        logger.info('Error! Data ({}) doesn\'t contain PK ({}).'.format(', '.join(row_data.keys()), pk_column))
        return False

    column_names = worksheet.row_values('1')
    if pk_column not in column_names:
        logger.info('Error! Column names ({}) doesn\'t contain PK ({}).'.format(', '.join(column_names), pk_column))
        return False

    col_positions = {}
    for column_name in column_names:
        col_positions[column_name] = column_names.index(column_name) + 1

    pk_value = row_data.get(pk_column, None)
    if pk_value:
        pk_cell = worksheet.find(pk_value)
    else:
        logger.info('Error! PK value ({}) is empty.'.format(pk_column))
        return False

    if pk_cell:
        if pk_cell.col == col_positions.get(pk_column):
            del row_data[pk_column]

            row_position = pk_cell.row
            for col_name, col_value in row_data.items():
                col_position = col_positions.get(col_name, None)
                if col_position:
                    worksheet.update_cell(row_position, col_position, col_value)
    else:
        new_row = []
        for col_name, col_position in col_positions.items():
            new_row.append(row_data.get(col_name))
        worksheet.append_row(new_row)
