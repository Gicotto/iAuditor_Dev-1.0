CREATE OR REPLACE TABLE DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_SCHEDULES_FLTN AS
SELECT
    TO_CHAR(TO_TIMESTAMP_TZ(DATA_DATE), 'YYYY-MM-DD HH24:MI:SS') AS EXPORTED_AT,
    RECORD_INSERTED_AT,
    DATA.value:id::string AS SCHEDULE_ID,
    DATA.value:description::string AS DESCRIPTION,
    DATA.value:recurrence::string AS RECURRENCE,
    DATA.value:duration::string AS DURATION,
    TO_CHAR(TO_TIMESTAMP_TZ(DATA.value:modified_at::string), 'YYYY-MM-DD HH24:MI:SS') AS MODIFIED_AT,
    TO_CHAR(TO_TIMESTAMP_TZ(DATA.value:from_date::string), 'YYYY-MM-DD HH24:MI:SS') AS FROM_DATE,
    TO_CHAR(TO_TIMESTAMP_TZ(DATA.value:to_date::string), 'YYYY-MM-DD HH24:MI:SS') AS TO_DATE,
    DATA.value:organisation_id::string AS ORGANISATION_ID,
    DATA.value:start_time_hour::int AS START_TIME_HOUR,
    DATA.value:start_time_minute::int AS START_TIME_MINUTE,
    LEFT(DATA.value:all_must_complete::string, 1) AS ALL_MUST_COMPLETE,
    DATA.value:status::string AS STATUS,
    DATA.value:timezone::string AS TIMEZONE,
    LEFT(DATA.value:can_late_submit::string, 1) AS CAN_LATE_SUBMIT,
    DATA.value:site_id::string AS SITE_ID,
    DATA.value:template_id::string AS TEMPLATE_ID,
    DATA.value:creator_user_id::string AS CREATOR_USER_ID,
FROM
    DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_SCHEDULES,
    LATERAL FLATTEN(input => JSON_PAYLOAD:data) AS DATA;
