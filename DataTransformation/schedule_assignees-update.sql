UPDATE DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_SCHEDULE_ASSIGNEES
SET PROCESSED = 't'
WHERE PROCESSED = 'f';