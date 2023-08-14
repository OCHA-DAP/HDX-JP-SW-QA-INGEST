import logging
import mock
from processing.helpers import Context
from processing.filtering import filter_out

from tests.util import fetch_values_wrapper

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


@mock.patch('processing.helpers.fetch_values', wraps=fetch_values_wrapper)
def test_filter_out(fetch_values_mock, context, clean_redis):
    event = _generate_test_event()
    filtered = filter_out(context, event)
    assert not filtered

    event_with_blocked_user = _generate_test_event()
    event_with_blocked_user['initiator_user_name'] = 'blocked-test-user'
    filtered_by_user = filter_out(context, event_with_blocked_user)
    assert filtered_by_user

    event_with_blocked_dataset = _generate_test_event()
    event_with_blocked_dataset['dataset_id'] = 'blocked-test-dataset-id'
    filtered_by_dataset = filter_out(context, event_with_blocked_dataset)
    assert filtered_by_dataset

    event_with_blocked_org = _generate_test_event()
    event_with_blocked_org['org_id'] = 'blocked-test-org-id'
    filtered_by_org = filter_out(context, event_with_blocked_org)
    assert filtered_by_org


@mock.patch('processing.helpers.fetch_values', wraps=fetch_values_wrapper)
def test_filter_out_caching(fetch_values_mock: mock.MagicMock, context, clean_redis):
    event = _generate_test_event()
    filtered = filter_out(context, event)
    uncached_call_count_to_gspreadsheets = fetch_values_mock.call_count

    # Next call should use the cache and not need google spreadsheets
    filtered = filter_out(context, event)
    cached_call_count_to_gspreadsheets = fetch_values_mock.call_count

    assert uncached_call_count_to_gspreadsheets == cached_call_count_to_gspreadsheets
