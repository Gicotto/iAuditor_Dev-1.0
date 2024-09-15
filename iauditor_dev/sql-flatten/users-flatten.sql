CREATE OR REPLACE TABLE DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_USERS_FLTN AS
SELECT
    TO_CHAR(TO_TIMESTAMP_TZ(DATA_DATE), 'YYYY-MM-DD HH24:MI:SS') AS EXPORTED_AT,
    DATA.value:id::string AS USER_ID,
    DATA.value:organisation_id::string AS ORGANISATION_ID,
    DATA.value:email::string AS EMAIL,
    DATA.value:firstname::string AS FIRST_NAME,
    DATA.value:lastname::string AS LAST_NAME,
    DATA.value:active::boolean AS ACTIVE,
    TO_CHAR(TO_TIMESTAMP_TZ(DATA.value:last_seen_at::string), 'YYYY-MM-DD HH24:MI:SS') AS LAST_SEEN_AT,
FROM
    DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_USERS,
    LATERAL FLATTEN(input => JSON_PAYLOAD:data) AS DATA;
