import polars as pl
from pathlib import Path

# Test mode
# False - relocates zip file from files to files/processed
# True - leaves zip in files to allow for repetitive testing with the same zip file
TEST_MODE = False

MOVE_SOURCE_FILE_AFTER_PROCESSING = True # True = moves source file to processed folder after processing. False = does not move source file
PULL_FILES_FROM_SOURCE = True  # True(use for prod) = pulls files from source every time. False = does not call API to get files
SAVE_TO_DATABASE = True  # True(use for prod) = TO SAVE TO DB. False = does not save to DB
DELETE_FILES_BEFORE_PROCESSING = True  # True = deletes files from ../files/processed/details after processing. False = does not delete files

RUN_SPECIAL_TEST_CONDITIONS = False

MY_TABLES_PREFIX = 'IAUDITOR_'
MODULE_NAME = 'IAUDITOR'
EXPORTER_BASE_DIR = Path(__file__).parent / '../../tools/'
EXPORTER_EXECUTABLE = EXPORTER_BASE_DIR / 'iauditor-exporter'
EXPORTER_FILES_DIR = EXPORTER_BASE_DIR / 'export/'
UNPROCESSED_FILES_DIR = Path(__file__).parent / '../../files/'

# fake comment

# General config
GENERAL_CONFIGURATION = {
    'log_level': {
        'fatal': True,
        'error': True,
        'warn': True,
        'info': True,
        'debug': False,
    },
    # user to truncate fields within specified files
    'truncate_list': {
        # 'client.dat': {  # client file
        #     3: 1,  # middleinitial(index 3) to 1 char
        #     18: 4000,  # notes(index 18) to 4000 chars
        # }
    },
}

# Restricted/approved list of allowed files/tables to processed.
# If we ever need to process new files, we have to:
#
# 1) Create the table schema
# 2) Include the name of the file in the list below(without the extension)

ALLOWED_NAMES_LIST = [
    'action_assignees',
    'actions',
    'groups',
    'group_users',
    'inspection_items',
    'inspections',
    'sites',
    'template_permissions',
    'client_alternate_names',  # not used
    'templates',
    'users',
    'schedules',
    'schedule_assignees',
    'schedule_occurrences',
    'action_timeline_items',
    'site_members',
    'action_timeline_items'
]

users_data_types = {
    'user_id' : pl.Utf8,
    'organisation' : pl.Utf8,
    'email' : pl.Utf8,
    'firstname' : pl.Utf8,
    'lastname' : pl.Utf8,
    'active' : pl.Utf8,
    'last_seen_at' : pl.Datetime,
    'exported_at' : pl.Datetime,
}

templates_data_types = {
    'template_id' : pl.Utf8,
    'archived' : pl.Utf8,
    'name' : pl.Utf8,
    'description' : pl.Utf8,
    'organisation_id' : pl.Utf8,
    'owner_name' : pl.Utf8,
    'owner_id' : pl.Utf8,
    'author_name' : pl.Utf8,
    'author_id' : pl.Utf8,
    'created_at' : pl.Datetime,
    'modified_at' : pl.Datetime,
    'exported_at' : pl.Datetime,
}

template_permissions_data_types = {
    'permission_id' : pl.Utf8,
    'template_id' : pl.Utf8,
    'permission' : pl.Utf8,
    'assignee_id' : pl.Utf8,
    'assignee_type' : pl.Utf8,
    'organisation_id' : pl.Utf8,
}

sites_data_types = {
    'site_id' : pl.Utf8,
    'name' : pl.Utf8,
    'creator_id' : pl.Utf8,
    'organisation_id' : pl.Utf8,
    'exported_at' : pl.Datetime,
    'deleted' : pl.Utf8,
    'site_uuid' : pl.Utf8,
    'meta_label' : pl.Utf8,
    'parent_id' : pl.Utf8,
}

schedules_data_types = {
    'schedule_id' : pl.Utf8,
    'description' : pl.Utf8,
    'reccurence' : pl.Utf8,
    'duration' : pl.Utf8,
    'modified_at' : pl.Datetime,
    'exported_at' : pl.Datetime,
    'from_date' : pl.Datetime,
    'to_date' : pl.Datetime,
    'start_time_hour' : pl.Int32,
    'start_time_minute' : pl.Int32,
    'all_must_complete' : pl.Utf8,
    'status' : pl.Utf8,
    'organisation_id' : pl.Utf8,
    'timezone' : pl.Utf8,
    'can_late_submit' : pl.Utf8,
    'site_id' : pl.Utf8,
    'template_id' : pl.Utf8,
    'creator_user_id' : pl.Utf8,
}

schedule_assignees_data_types = {
    'id' : pl.Utf8,
    'schedule_id' : pl.Utf8,
    'assignee_id' : pl.Utf8,
    'organisation_id' : pl.Utf8,
    'type' : pl.Utf8,
    'name' : pl.Utf8,
    'exported_at' : pl.Datetime,
}

inspections_data_types = {
    'audit_id' : pl.Utf8,
    'name' : pl.Utf8,
    'archived' : pl.Utf8,
    'owner_name' : pl.Utf8,
    'owner_id' : pl.Utf8,
    'author_name' : pl.Utf8,
    'author_id' : pl.Utf8,
    'score' : pl.Float64,
    'max_score' : pl.Float64,
    'score_percentage' : pl.Float64,
    'duration' : pl.Int64,
    'template_id' : pl.Utf8,
    'organisation_id' : pl.Utf8,
    'template_name' : pl.Utf8,
    'template_author' : pl.Utf8,
    'site_id' : pl.Utf8,
    'date_started' : pl.Datetime,
    'date_completed' : pl.Datetime,
    'date_modified' : pl.Datetime,
    'created_at' : pl.Datetime,
    'modified_at' : pl.Datetime,
    'exported_at' : pl.Datetime,
    'document_no' : pl.Utf8,
    'prepared_by' : pl.Utf8,
    'location' : pl.Utf8,
    'conducted_on' : pl.Datetime,
    'personnel' : pl.Utf8,
    'client_site' : pl.Utf8,
    'latitude' : pl.Utf8,
    'longitude' : pl.Utf8,
    'web_report_link' : pl.Utf8,
    'deleted' : pl.Utf8,
    'asset_id' : pl.Utf8,
}

groups_data_types = {
    'group_id' : pl.Utf8,
    'name' : pl.Utf8,
    'organisation_id' : pl.Utf8,
    'exported_at' : pl.Datetime,
}

group_users_data_types = {
    'user_id' : pl.Utf8,
    'group_id' : pl.Utf8,
    'organisation_id' : pl.Utf8,
    'exported_at' : pl.Datetime,
}

actions_data_types = {
    'action_id' : pl.Utf8,
    'title' : pl.Utf8,
    'description' : pl.Utf8,
    'site_id' : pl.Utf8,
    'priority' : pl.Utf8,
    'status' : pl.Utf8,
    'due_date' : pl.Datetime,
    'created_at' : pl.Datetime,
    'modified_at' : pl.Datetime,
    'exported_at' : pl.Datetime,
    'creator_user_id' : pl.Utf8,
    'creator_user_name' : pl.Utf8,
    'template_id' : pl.Utf8,
    'audit_id' : pl.Utf8,
    'audit_title' : pl.Utf8,
    'audit_item_id' : pl.Utf8,
    'audit_item_label' : pl.Utf8,
    'organisation_id' : pl.Utf8,
    'completed_at' : pl.Datetime,
    'action_label' : pl.Utf8,
    'deleted' : pl.Utf8,
    'asset_id' : pl.Utf8,
}

action_timeline_items_data_types = {
    'item_id': pl.Utf8,
    'task_id': pl.Utf8,
    'organisation_id': pl.Utf8,
    'task_creator_id': pl.Utf8,
    'task_creator_name': pl.Utf8,
    'timestamp': pl.Datetime,
    'creator_id': pl.Utf8,
    'creator_name': pl.Utf8,
    'item_type': pl.Utf8,
    'item_data': pl.Utf8,
}

action_assignees_data_types = {
    'id': pl.Utf8,  # Looks like a complex string, possibly an identifier with embedded UUID
    'action_id': pl.Utf8,  # UUID
    'assignee_id': pl.Utf8,  # Identifier with UUID embedded
    'type': pl.Utf8, # Simple string, possibly a tag or category
    'name': pl.Utf8, # Simple string, possibly a tag or category
    'organisation_id': pl.Utf8, # Role ID with UUID
    'modified_at': pl.Datetime, # ISO 8601 datetime
    'exported_at': pl.Datetime, # High precision ISO 8601 datetime
}

inspection_items_data_types = {
    'id': pl.Utf8,  # Looks like a complex string, possibly an identifier with embedded UUID
    'item_id': pl.Utf8,  # UUID
    'audit_id': pl.Utf8,  # Identifier with UUID embedded
    'item_index': pl.Int32, # Integer number
    'template_id': pl.Utf8,  # Appears as another identifier or template ID
    'parent_id': pl.Utf8,  # UUID
    'created_at': pl.Datetime, # ISO 8601 datetime
    'modified_at': pl.Datetime, # ISO 8601 datetime
    'exported_at': pl.Datetime, # High precision ISO 8601 datetime
    'type': pl.Utf8, # Simple string, possibly a tag or category
    'category': pl.Utf8, # Simple string, possibly a tag or category
    'category_id': pl.Utf8, # UUID repeated, consistency assumed
    'organisation_id': pl.Utf8, # Role ID with UUID
    'parent_ids': pl.Utf8, # UUID repeated, consistency assumed
    'label': pl.Utf8, # Descriptive text
    'response': pl.Utf8, # Simple categorical string
    'response_id': pl.Utf8, # UUID
    'response_set_id': pl.Utf8, # UUID
    'is_failed_response': pl.Utf8, # Boolean value
    'icomment': pl.Utf8, # Use Utf8 for potential missing data
    'media_files': pl.Utf8, # Use Utf8 for potential missing data
    'media_ids': pl.Utf8, # Use Utf8 for potential missing data
    'media_hypertext_reference': pl.Utf8, # Use Utf8 for potential missing data
    'score': pl.Float64, # Integer, potentially small numbers
    'max_score': pl.Int32, # Integer, potentially small numbers
    'score_percentage': pl.Float64, # Integer, might be used for percentages or scores
    'combined_score': pl.Float64, # Integer, potentially small numbers
    'combined_max_score': pl.Int32, # Integer, potentially small numbers
    'combined_score_percentage': pl.Float64, # Integer, potentially small numbers
    'mandatory': pl.Utf8, # Boolean value
    'inactive': pl.Utf8, # Boolean value
    'location_latitude': pl.Utf8, # Use Utf8 for potential missing data
    'location_longitude': pl.Utf8  # Use Utf8 for potential missing data
}

site_members_data_types = {
    'site_id': pl.Utf8,
    'member_id': pl.Utf8,
    'exported_at': pl.Datetime,
}

file_config = {
    'actions': {
        'table_name': 'IAUDITOR_ACTIONS',
        'data_types': actions_data_types,
        'file_name': 'actions.dat',
        'date_columns': ['due_date', 'created_at', 'modified_at', 'exported_at', 'completed_at']
    },
    'action_timeline_items': {
        'table_name': 'IAUDITOR_ACTION_TIMELINE_ITEMS',
        'data_types': action_timeline_items_data_types,
        'file_name': 'action_timeline_items.dat',
        'date_columns': ['timestamp']
    },
    'action_assignees': {
        'table_name': 'IAUDITOR_ACTION_ASSIGNEES',
        'data_types': action_assignees_data_types,
        'file_name': 'action_assignees.dat',
        'date_columns': ['modified_at', 'exported_at']
    },
    'groups': {
        'table_name': 'IAUDITOR_GROUPS',
        'data_types': groups_data_types,
        'file_name': 'groups.dat',
        'date_columns': ['exported_at']
    },
    'group_users': {
        'table_name': 'IAUDITOR_GROUP_USERS',
        'data_types': group_users_data_types,
        'file_name': 'group_users.dat',
        'date_columns': ['exported_at']
    },
    'inspection_items': {
        'table_name': 'IAUDITOR_INSPECTION_ITEMS',
        'data_types': inspection_items_data_types,
        'file_name': 'inspection_items.dat',
        'date_columns': ['created_at', 'modified_at', 'exported_at']
    },
    'inspections': {
        'table_name': 'IAUDITOR_INSPECTIONS',
        'data_types': inspections_data_types,
        'file_name': 'inspections.dat',
        'date_columns': ['date_started', 'date_completed', 'date_modified', 'created_at', 'modified_at', 'exported_at', 'conducted_on']
    },
    'sites': {
        'table_name': 'IAUDITOR_SITES',
        'data_types': sites_data_types,
        'file_name': 'sites.dat',
        'date_columns': ['exported_at']
    },
    'site_members': {
        'table_name': 'IAUDITOR_SITE_MEMBERS',
        'data_types': site_members_data_types,
        'file_name': 'site_members.dat',
        'date_columns': ['exported_at']
    },
    'template_permissions': {
        'table_name': 'IAUDITOR_TEMPLATE_PERMISSIONS',
        'data_types': template_permissions_data_types,
        'file_name': 'template_permissions.dat',
        'date_columns': []
    },
    'templates': {
        'table_name': 'IAUDITOR_TEMPLATES',
        'data_types': templates_data_types,
        'file_name': 'templates.dat',
        'date_columns': ['created_at', 'modified_at', 'exported_at']
    },
    'users': {
        'table_name': 'IAUDITOR_USERS',
        'data_types': users_data_types,
        'file_name': 'users.dat',
        'date_columns': ['last_seen_at', 'exported_at']
    },
    'schedules': {
        'table_name': 'IAUDITOR_SCHEDULES',
        'data_types': schedules_data_types,
        'file_name': 'schedules.dat',
        'date_columns': ['modified_at', 'exported_at', 'from_date', 'to_date']
    },
    'schedule_assignees': {
        'table_name': 'IAUDITOR_SCHEDULE_ASSIGNEES',
        'data_types': schedule_assignees_data_types,
        'file_name': 'schedule_assignees.dat',
        'date_columns': ['exported_at']
    },
}