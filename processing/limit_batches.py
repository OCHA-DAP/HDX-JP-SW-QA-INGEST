import json
import logging
import time
from typing import Dict, List
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
    dataset_id = event.get('dataset_id')
    owner_org_id = event.get('org_id', '')
    if not owner_org_id:
        logger.error(f'Empty org_id for event {event.get("event_type")} for dataset {event.get("dataset_name")}')
    org_counter_key = 'org-counter-' + owner_org_id

    # We only want to write to Google Sheets every 'batches_duration_between_writing' minutes
    batches_duration_between_writing = 1
    batches_timestamp_redis_key = 'google_spreadsheet_delay_timestamp_batches'
    batches_datasets_redis_key = 'google_spreadsheet_datasets_map_key_batches'
    batches_datasets_fields = ['dataset_id', 'dataset_name', 'org_id', 'org_name', 'event_type', 'event_time']

    output_4_redis = _get_output_4_redis(context, dataset_id, org_counter_key, timestamp)

    context.store.set_object(output_4_redis['key'], output_4_redis['value'])

    return_value = False

    if len(
            output_4_redis.get('value', None).get('datasets_list', [])) <= context.config.MAX_ENTRIES_LIMIT_BATCHES - 1:
        return_value = False
    # We have TOO MANY dataset events for the same org in the last DURATION_MINUTES_LIMIT_BATCHES minutes.
    # We need to block these events
    else:
        cached_data = {key: event[key] for key in batches_datasets_fields}
        limiting_message = f'Limiting N8N/JIRA processing for org "{event["org_name"]}" because of too many events. ' + \
                        f'Blocked event for dataset "{event["dataset_name"]}"'
        logger.warning(limiting_message)
        # context.slack_client.post_to_slack_channel(limiting_message)

        # if there's no batch of datasets in redis then we need to update the timestamp to say that we're starting now
        if not context.store.exists(batches_datasets_redis_key) or not context.store.exists(batches_timestamp_redis_key):
            context.store.set_object(batches_timestamp_redis_key, timestamp)
        # push the new dataset dict
        context.store.set_map(batches_datasets_redis_key, {event.get('dataset_id'): json.dumps(cached_data)})
        return_value = True


    # Always check if there's something to push to 'Too Many Datasets' google sheet
    redis_timestamp = context.store.get_object(batches_timestamp_redis_key)
    if redis_timestamp:
        expired = (timestamp - redis_timestamp) > batches_duration_between_writing * 60
        if expired:
            # update timestamp in Redis
            context.store.set_object(batches_timestamp_redis_key, timestamp)
            # read with POP from Redis
            datasets_map = context.store.get_map_and_delete(batches_datasets_redis_key)
            if datasets_map:
                dataset_infos = [json.loads(item) for item in datasets_map.values()]
                list_of_datasets_as_string = ', '.join((item['dataset_name'] for item in dataset_infos))
                message_too_many_datasets = 'The following datasets will be pushed to the "Too Many Datasets" ' + \
                                            f'google sheet: {list_of_datasets_as_string}'
                logger.info(message_too_many_datasets)
                context.slack_client.post_to_slack_channel(message_too_many_datasets)
                # write to spreadsheet
                _process_notification(context, dataset_infos)

    return return_value


def _process_notification(context: Context, events: List):
    """

    :param context:
    :param events:
    """
    rows = []
    for event in events:
        rows.append(_populate_row_data(event))
    _update_gsheet(context.gsheets, context.config.SPREADSHEET_NAME, context.config.SHEET_NAME_LIMIT_BATCHES,
                   context.config.COL_NAME_LIMIT_BATCHES, rows)


def _populate_row_data(event: Dict):
    """

    :param event:
    :return:
    """
    row_data = {
        'id': event.get('dataset_id'),
        'name': event.get('dataset_name'),
        'owner_org': event.get('org_id'),
        'organization_name': event.get('org_name'),
        'event_type': event.get('event_type'),
        'event_time': event.get('event_time')
    }
    return row_data


def _get_output_4_redis(context: Context, dataset_id: str, org_counter_key: str, timestamp: float) -> Dict:
    """

    :param context:
    :param dataset_id:
    :param org_counter_key:
    :param timestamp:
    :return:
    """
    if context.store.exists(org_counter_key):
        org_counter_redis = context.store.get_object(org_counter_key)
        datasets_list = []
        for item in org_counter_redis.get('datasets_list'):
            if dataset_id != item.get('id') and timestamp - item.get(
                    'timestamp') < 60 * context.config.DURATION_MINUTES_LIMIT_BATCHES:
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
                "datasets_list": [{"id": dataset_id, "timestamp": timestamp}]
            }
        }
    return output_4_redis


def _update_gsheet(gc: Client, spreadsheet_name: str, sheet_name: str, pk_column: str, rows_data: List) -> bool:
    """

    :param gc:
    :param spreadsheet_name:
    :param sheet_name:
    :param pk_column:
    :param rows_data:
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

    column_names = worksheet.row_values('1')
    if pk_column not in column_names:
        logger.info('Error! Column names ({}) doesn\'t contain PK ({}).'.format(', '.join(column_names), pk_column))
        return False

    col_positions = {}
    for column_name in column_names:
        col_positions[column_name] = column_names.index(column_name) + 1

    new_rows = []
    for row_data in rows_data:
        if pk_column not in row_data.keys():
            logger.info('Error! Data ({}) doesn\'t contain PK ({}).'.format(', '.join(row_data.keys()), pk_column))
            return False

        new_row = []
        for col_name, col_position in col_positions.items():
            new_row.append(row_data.get(col_name))
        new_rows.append(new_row)

    worksheet.append_rows(new_rows)

    return True
