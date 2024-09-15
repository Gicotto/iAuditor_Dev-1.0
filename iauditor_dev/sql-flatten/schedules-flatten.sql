CREATE OR REPLACE TABLE DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_SCHEDULES_FLTN AS
SELECT
    DATA_DATE,
    RECORD_INSERTED_AT,
    DATA.value:id::string AS SCHEDULE_ID,
    DATA.value:description::string AS DESCRIPTION,
    DATA.value:recurrence::string AS RECURRENCE,
    DATA.value:duration::string AS DURATION,
    DATA.value:modified_at::string AS MODIFIED_AT,
    DATA.value:organisation_id::string AS ORGANISATION_ID,
    DATA.value:from_date::string AS FROM_DATE,
    DATA.value:to_date::string AS TO_DATE,
    DATA.value:start_time_hour::int AS START_TIME_HOUR,
    DATA.value:start_time_minute::int AS START_TIME_MINUTE,
    DATA.value:all_must_complete::boolean AS ALL_MUST_COMPLETE,
    DATA.value:status::string AS STATUS,
    DATA.value:timezone::string AS TIMEZONE,
    DATA.value:can_late_submit::boolean AS CAN_LATE_SUBMIT,
    DATA.value:site_id::string AS SITE_ID,
    DATA.value:template_id::string AS TEMPLATE_ID,
    DATA.value:creator_user_id::string AS CREATOR_USER_ID,
FROM
    DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_SCHEDULES,
    LATERAL FLATTEN(input => JSON_PAYLOAD:data) AS DATA;
