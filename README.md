# QA Ingestion / Pre-processor

Processes the events before they are used by the N8N QA workflow.

---

## Pre-processing walkthrough

After connecting to the HDX Event Bus, it listens for events and processes incoming changes up to a defined maximum
number of iterations, skipping unallowed event types.

### Filter out safelists `filter_out()`

Checks each event against the safelists and skips the events that **are** on the `dataset`, `org` or `user` list. Events
on the safelists don't move forward for processing in n8n.

### Limit bulk changes `limit()`

Checks if we have too many dataset events for the same organization in a given period of minutes and blocks them.

This writes the discarded events to the "Too Many Datasets" Google Sheet at a set time interval to avoid limitations.

### Flag items from watchlists `flag_if_on_watchlist()`

Adds an `n8nFlags` object to each event. This will be leveraged later in the workflow to store new info on the event as
it is processed.

Iterates all of the change events and adds `n8nFlags.onOrgWatchlist` and/or `n8nFlags.onUserWatchlist` for any events on
the lists. This will be used in the Jira ticketing workflow to call QA officer attention to watchlist items via Jira
labels and rich text formatting in the Task descriptions.

### Populate changed_fields `populate_changed_fields()`

Adds an empty `changed_fields` array for deletion events (`resource-deleted` and `dataset-deleted`). These events don't
have changed fields, but we add it here to help with processing later.

### Split each dataset metadata field change into its own event `split_each_field_own_event()`

If the change event is a `dataset-created` or `dataset-metadata-changed` event with multiple changed fields, they are
split into unique events for each field. Later in the Jira Ticketing Workflow each dataset `changed_field` will be
processed atomically to create Jira Sub-Tasks and Comments.

### Add the redis key to each item `populate_with_redis_key()`

Append the Redis key to the event. Redis stores these objects 12 hours by default. They're deleted later after
the Jira Ticketing Workflow has processed them so we need to retain the key as a reference.

### Store object

The event is saved to Redis for processing in the Jira Ticketing Workflow. 
