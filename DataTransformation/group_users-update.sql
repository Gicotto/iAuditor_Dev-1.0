UPDATE DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_GROUP_USERS
SET PROCESSED = 't'
WHERE PROCESSED = 'f';