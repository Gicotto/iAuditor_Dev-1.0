CREATE OR REPLACE TABLE DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_USERS_FLTN AS
SELECT
    DATA_DATE,
    RECORD_INSERTED_AT,
    DATA.value:id::string AS USER_ID,
    DATA.value:organisation_id::string AS ORGANISATION_ID,
    DATA.value:email::string AS EMAIL,
    DATA.value:firstname::string AS FIRST_NAME,
    DATA.value:lastname::string AS LAST_NAME,
    DATA.value:active::boolean AS ACTIVE,
    DATA.value:last_seen_at::string AS LAST_SEEN_AT,
FROM
    DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_USERS,
    LATERAL FLATTEN(input => JSON_PAYLOAD:data) AS DATA;
