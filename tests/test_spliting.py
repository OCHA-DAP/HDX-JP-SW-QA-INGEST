import logging
import mock
from processing.spliting import split_each_field_own_event

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


def _generate_resource_created_test_event():
    return {
        "event_type": "resource-cread",
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
                "old_value": "",
                "old_display_value": "",
            },
            {
                "field": "name",
                "new_value": "New title",
                "new_display_value": "New title",
                "old_value": "",
                "old_display_value": "",
            },
        ],
        "org_id": "test-org-id",
        "org_name": "test-org",
        "org_title": "Test Org",
    }


def test_valid_split_fields(context, clean_redis):
    event = _generate_test_event()
    events = split_each_field_own_event(context, event)

    assert len(event.get('changed_fields')) == len(events)
    assert event.get('changed_fields')[0]['field'] == events[0]['changed_fields'][0]['field']
    assert event.get('changed_fields')[1]['field'] == events[1]['changed_fields'][0]['field']
