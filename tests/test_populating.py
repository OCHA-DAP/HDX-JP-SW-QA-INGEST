import logging
import mock
from processing.populating import populate_changed_fields, populate_with_redis_key

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
            }
        ],
        "org_id": "test-org-id",
        "org_name": "test-org",
        "org_title": "Test Org",
    }


def _generate_dataset_deleted_test_event():
    return {
        "event_type": "dataset-deleted",
        "event_time": "2023-08-02T20:40:49.402615",
        "event_source": "ckan",
        "initiator_user_name": "test-user",
        "dataset_name": "test-dataset",
        "dataset_title": "Test dataset",
        "dataset_id": "test-dataset-id",
        "org_id": "test-org-id",
        "org_name": "test-org",
        "org_title": "Test Org",
    }


def _generate_resource_deleted_test_event():
    return {
        "event_type": "resource-deleted",
        "event_time": "2023-08-02T20:40:49.402615",
        "event_source": "ckan",
        "initiator_user_name": "test-user",
        "dataset_name": "test-dataset",
        "dataset_title": "Test dataset",
        "dataset_id": "test-dataset-id",
        "org_id": "test-org-id",
        "org_name": "test-org",
        "org_title": "Test Org",
    }


def test_valid_populate_changed_fields(context, clean_redis):
    dataset_deleted_event = _generate_dataset_deleted_test_event()
    resource_deleted_event = _generate_resource_deleted_test_event()

    assert 'changed_fields' not in dataset_deleted_event
    assert 'changed_fields' not in resource_deleted_event

    dataset_populated_event = populate_changed_fields(dataset_deleted_event)
    resource_populated_event = populate_changed_fields(resource_deleted_event)

    assert 'changed_fields' in dataset_populated_event, 'event should be populated with changed_fields'
    assert 'changed_fields' in resource_populated_event, 'event should be populated with changed_fields'


def test_invalid_populate_changed_fields(context, clean_redis):
    event = _generate_test_event()

    populated_event = populate_changed_fields(event)

    assert populated_event.get('changed_fields', False), \
        'event should not be populated with changed_fields because it\'s not a dataset/resource deleted event type'


def test_populate_with_redis_key(context, clean_redis):
    event = _generate_test_event()

    populated_event = populate_with_redis_key(context.config, event)

    assert populated_event.get('redisKey', False), 'event should be populated with redisKey'
    assert event['event_time'] in populated_event['redisKey']
    assert event['event_type'] in populated_event['redisKey']
    assert event['event_source'] in populated_event['redisKey']
    assert event['changed_fields'][0]['field'] in populated_event['redisKey']
