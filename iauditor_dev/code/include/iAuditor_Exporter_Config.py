import datetime
import os

current_time = str(datetime.datetime.now())

# Current directory
current_dir = os.path.dirname(__file__)
snowflake_config = os.path.join(current_dir, '../../code/include/config/config.ini')

exporter_config = 'iAuditor-Exporter-config.ini'


table_prefix = "IAUDITOR_"
log_columns = ["BATCH_ID", "LOG_TYPE", "MESSAGE", "RECORD_INSERTED_AT"]
logs_table_name = "IAUDITOR_EXECUTION_LOGS"


sql_scripts = [
    "action_assignees-flatten.sql",
    "action_timeline_items-flatten.sql",
    "actions-flatten.sql",
    "group_users-flatten.sql",
    "groups-flatten.sql",
    "inspection_items-flatten.sql",
    "inspections-flatten.sql",
    "schedule_assignees-flatten.sql",
    "schedules-flatten.sql",
    "site_members-flatten.sql",
    "sites-flatten.sql",
    "template_permissions-flatten.sql",
    "templates-flatten.sql",
    "users-flatten.sql",
 ]

urls = {
    "actions": "https://api.safetyculture.io/feed/actions",
    "action_assignees": "https://api.safetyculture.io/feed/action_assignees",
    "action_timeline_items": "https://api.safetyculture.io/feed/action_timeline_items",
    "groups": "https://api.safetyculture.io/feed/groups",
    "group_users": "https://api.safetyculture.io/feed/group_users",
    "inspections": "https://api.safetyculture.io/feed/inspections",
    "inspection_items": "https://api.safetyculture.io/feed/inspection_items",
    "schedules": "https://api.safetyculture.io/feed/schedules",
    "schedule_assignees": "https://api.safetyculture.io/feed/schedule_assignees",
    "sites": "https://api.safetyculture.io/feed/sites",
    "site_members": "https://api.safetyculture.io/feed/site_members",
    "templates": "https://api.safetyculture.io/feed/templates",
    "template_permissions": "https://api.safetyculture.io/feed/template_permissions",
    "users": "https://api.safetyculture.io/feed/users",
}

keep_url = ["https://api.safetyculture.io/feed/schedule_assignees", "https://api.safetyculture.io/feed/users"]

