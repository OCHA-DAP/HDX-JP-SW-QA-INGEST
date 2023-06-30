import logging
import time
from typing import Dict
from processing.helpers import Context
from gspread.client import Client
from gspread.exceptions import APIError, WorksheetNotFound

logger = logging.getLogger(__name__)


def limit(context: Context, event: Dict) -> bool:
    """

    :param context:
    :param event:
    :return: False if ingest flow can continue, True if notifications was sent to google spreadsheet
    """
    timestamp = time.time()
    dataset_obj = event.get('dataset_obj')
    dataset_id = event.get('dataset_obj').get('id')
    owner_org_id = dataset_obj.get('owner_org') or ''
    if not owner_org_id:
        logger.error(f'Empty org_id for event {event.get("event_type")} for dataset {event.get("dataset_name")}')
    org_counter_key = 'org-counter-' + owner_org_id

    output_4_redis = _get_output_4_redis(context, dataset_id, dataset_obj, org_counter_key, timestamp)

    context.store.set_object(output_4_redis['key'], output_4_redis['value'])

    if len(output_4_redis.get('value', None).get('datasets_list', [])) <= context.config.MAX_ENTRIES_LIMIT_BATCHES-1:
        return False
    else:
        # write to google doc
        _process_notification(context, event)
        return True


def _process_notification(context: Context, event: Dict):
    """

    :param context:
    :param event:
    """
    row_data = {
        'id': event.get('dataset_id'),
        'name': event.get('dataset_name'),
        'package_creator': event.get('dataset_obj').get('package_creator'),
        'updated_by_script': event.get('dataset_obj').get('updated_by_script'),
        'owner_org': event.get('dataset_obj').get('owner_org'),
        'organization_name': event.get('dataset_obj').get('organization').get('name'),
        'title': event.get('dataset_obj').get('title'),
        'data_update_frequency': event.get('dataset_obj').get('data_update_frequency'),
        'maintainer': event.get('dataset_obj').get('maintainer'),
        'metadata_created': event.get('dataset_obj').get('metadata_created'),
        'metadata_modified': event.get('dataset_obj').get('metadata_modified'),
        'dataset_date': event.get('dataset_obj').get('dataset_date'),
        'event_type': event.get('event_type'),
        'event_time': event.get('event_time')
    }
    _update_gsheet(context.gsheets, context.config.SPREADSHEET_NAME, context.config.SHEET_NAME_LIMIT_BATCHES,
                   context.config.COL_NAME_LIMIT_BATCHES, row_data)


def _get_output_4_redis(context: Context, dataset_id: str, dataset_obj: Dict, org_counter_key: str,
                        timestamp: float) -> Dict:
    """

    :param context:
    :param dataset_id:
    :param dataset_obj:
    :param org_counter_key:
    :param timestamp:
    :return:
    """
    if context.store.exists(org_counter_key):
        org_counter_redis = context.store.get_object(org_counter_key)
        datasets_list = []
        for item in org_counter_redis.get('datasets_list'):
            if dataset_id != item.get('id') and timestamp - item.get(
                    'timestamp') < 60 * context.config.DURATION_MINUTES_LIMIT_BATCHES * 1000:
                datasets_list.append(item)
        datasets_list.append({"id": dataset_id, "timestamp": timestamp})
        datasets_list = datasets_list[-context.config.MAX_ENTRIES_LIMIT_BATCHES:]
        output_4_redis = {
            "key": org_counter_key,
            "value": {"datasets_list": datasets_list}
        }
    else:
        output_4_redis = {
            "key": org_counter_key,
            "value": {
                "datasets_list": [{"id": dataset_obj.get('id'),
                                   "timestamp": timestamp}]
            }
        }
    return output_4_redis


def _update_gsheet(gc: Client, spreadsheet_name: str, sheet_name: str, pk_column: str, row_data: Dict) -> bool:
    """

    :param gc:
    :param spreadsheet_name:
    :param sheet_name:
    :param pk_column:
    :param row_data:
    :return:
    """
    try:
        sh = gc.open(spreadsheet_name)
    except APIError:
        logger.info('Error! Invalid spreadsheet name ({}).'.format(spreadsheet_name))
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

    return True
