import logging
import mock
from processing.helpers import Context
from processing.watchlists import flag_if_on_watchlist

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
def test_flag_if_on_watchlist(fetch_values_mock, context, clean_redis):
    event = _generate_test_event()
    assert not event.get('n8nFlags')
    flag_if_on_watchlist(context, event)
    assert event.get('n8nFlags')
    assert event.get('n8nFlags').get('onUserWatchlist') is not True
    assert event.get('n8nFlags').get('onOrgWatchlist') is not True

    event_with_watchlisted_user = _generate_test_event()
    event_with_watchlisted_user['initiator_user_name'] = 'watchlisted-test-user'
    flag_if_on_watchlist(context, event_with_watchlisted_user)
    assert event_with_watchlisted_user.get('n8nFlags').get('onUserWatchlist', False) is True

    event_with_watchlisted_org = _generate_test_event()
    event_with_watchlisted_org['org_id'] = 'watchlisted-test-org-id'
    flag_if_on_watchlist(context, event_with_watchlisted_org)
    assert event_with_watchlisted_org.get('n8nFlags').get('onOrgWatchlist', False) is True
