import logging
import mock
import time
from processing.limit_batches import limit, _get_output_4_redis, _populate_row_data

logger = logging.getLogger(__name__)


def _generate_test_event():
    return {
        "event_type": "dataset-metadata-changed",
        "event_time": "2023-08-02T20:40:49.402615",
        "event_source": "ckan",
        "initiator_user_name": "test-user",
        "dataset_name": "test-dataset",
        "dataset_title": "Test dataset",
        "dataset_id": "test-dataset-id",
        "changed_fields": [
            {
                "field": "notes",
                "new_value": "New description",
                "new_display_value": "New description",
                "old_value": "Old description",
                "old_display_value": "Old description",
            },
            {
                "field": "tags",
                "added_items": [],
                "removed_items": ["removed-test-tag"],
            },
        ],
        "org_id": "test-org-id",
        "org_name": "test-org",
        "org_title": "Test Org",
    }


def test_valid_limit_batches(context, clean_redis):
    max_events = context.config.MAX_ENTRIES_LIMIT_BATCHES
    no_events = 8
    should_limit = False
    for index in range(no_events):
        event = _generate_test_event()
        event['dataset_name'] = '%s_%s' % (event['dataset_name'], index)
        event['dataset_title'] = '%s_%s' % (event['dataset_title'], index)
        event['dataset_id'] = '%s_%s' % (event['dataset_id'], index)
        should_limit = limit(context, event)

        timestamp = time.time()
        output_4_redis = _get_output_4_redis(context, event['dataset_id'], 'org-counter-' + event['org_id'], timestamp)
        assert any(d['id'] == event['dataset_id'] for d in output_4_redis.get('value', None).get('datasets_list', []))

    if max_events <= no_events:
        assert should_limit is True, 'batch should be limited'
    else:
        assert should_limit is False, 'batch shouldn\'t be limited'


def test_invalid_limit_batches_diff_org(context, clean_redis):
    no_events = 8
    should_limit = False
    for index in range(no_events):
        event = _generate_test_event()
        event['dataset_name'] = '%s_%s' % (event['dataset_name'], index)
        event['dataset_title'] = '%s_%s' % (event['dataset_title'], index)
        event['dataset_id'] = '%s_%s' % (event['dataset_id'], index)

        event['org_id'] = '%s_%s' % (event['org_id'], index)
        should_limit = limit(context, event)

    assert should_limit is False, 'batch shouldn\'t be limited because we have diff orgs'


def test_gsheets_row_content(context, clean_redis):
    event = _generate_test_event()
    row = _populate_row_data(event)

    assert len(row) == 6
    assert 'id' in row
    assert 'name' in row
    assert 'owner_org' in row
    assert 'organization_name' in row
    assert 'event_type' in row
    assert 'event_time' in row
