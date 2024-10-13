import datetime
import os

current_time = str(datetime.datetime.now())

# Current directory
current_dir = os.path.dirname(__file__)

# Run Settings
upload_logs = True
upload_snowflake = True
historical_last_call = True

run_exporter = True
run_processor = True

table_prefix = "IAUDITOR_"
log_columns = ["BATCH_ID", "LOG_TYPE", "MESSAGE", "RECORD_INSERTED_AT"]

sql_scripts_dict = {
    "action_assignees": {
        "flatten": "action_assignees-flatten.sql",
        "update": "action_assignees-update.sql"
    },
    "action_timeline_items": {
        "flatten": "action_timeline_items-flatten.sql",
        "update": "action_timeline_items-update.sql"
    },
    "actions": {
        "flatten": "actions-flatten.sql",
        "update": "actions-update.sql"
    },
    "group_users": {
        "flatten": "group_users-flatten.sql",
        "update": "group_users-update.sql"
    },
    "groups": {
        "flatten": "groups-flatten.sql",
        "update": "groups-update.sql"
    },
    "inspection_items": {
        "flatten": "inspection_items-flatten.sql",
        "update": "inspection_items-update.sql"
    },
    "inspections": {
        "flatten": "inspections-flatten.sql",
        "update": "inspections-update.sql"
    },
    "schedule_assignees": {
        "flatten": "schedule_assignees-flatten.sql",
        "update": "schedule_assignees-update.sql"
    },
    "schedules": {
        "flatten": "schedules-flatten.sql",
        "update": "schedules-update.sql"
    },
    "site_members": {
        "flatten": "site_members-flatten.sql",
        "update": "site_members-update.sql"
    },
    "sites": {
        "flatten": "sites-flatten.sql",
        "update": "sites-update.sql"
    },
    "template_permissions": {
        "flatten": "template_permissions-flatten.sql",
        "update": "template_permissions-update.sql"
    },
    "templates": {
        "flatten": "templates-flatten.sql",
        "update": "templates-update.sql"
    },
    "users": {
        "flatten": "users-flatten.sql",
        "update": "users-update.sql"
    }
}

urls = {
    'actions': {
        'endpoint': 'https://api.safetyculture.io/feed/actions',
        'name': 'actions'
        },
    "action_assignees": {
        'endpoint': "https://api.safetyculture.io/feed/action_assignees",
        'name': 'action_assignees'
        },
    "action_timeline_items": {
        'endpoint': "https://api.safetyculture.io/feed/action_timeline_items",
        'name': 'action_timeline_items'
        },
    "groups": {
        'endpoint': "https://api.safetyculture.io/feed/groups",
        'name': 'groups'
               },
    "group_users": {
        'endpoint': "https://api.safetyculture.io/feed/group_users",
        'name': 'group_users'
        },
    "inspections": {
        'endpoint': "https://api.safetyculture.io/feed/inspections",
        'name': 'inspections'
        },
    "inspection_items": {
        'endpoint': "https://api.safetyculture.io/feed/inspection_items",
        'name': 'inspection_items'
        },
    "schedules": {
        'endpoint': "https://api.safetyculture.io/feed/schedules",
        'name': 'schedules'
        },
    "schedule_assignees": {
        'endpoint': "https://api.safetyculture.io/feed/schedule_assignees",
        'name': 'schedule_assignees'
        },
    "sites": {
        'endpoint': "https://api.safetyculture.io/feed/sites",
        'name': 'sites'
        },
    "site_members": {
        'endpoint': "https://api.safetyculture.io/feed/site_members",
        'name': 'site_members'           
        },
    "templates": {
        'endpoint': "https://api.safetyculture.io/feed/templates",
        'name': 'templates'
        },
    "template_permissions": {
        'endpoint': "https://api.safetyculture.io/feed/template_permissions",
        'name': 'template_permissions'
        },
    "users": {
        'endpoint': "https://api.safetyculture.io/feed/users",
        "name": "users"
        }
}

keep_url = ["https://api.safetyculture.io/feed/schedule_assignees", "https://api.safetyculture.io/feed/users"]

base_api_url = "https://api.safetyculture.io"