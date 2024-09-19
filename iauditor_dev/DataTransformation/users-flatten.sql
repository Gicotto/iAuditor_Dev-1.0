INSERT INTO DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_USERS_FLTN (
    USER_ID,
    ORGANISATION_ID,
    EMAIL,
    FIRST_NAME,
    LAST_NAME,
    ACTIVE,
    LAST_SEEN_AT,
    EXPORTED_AT
)
SELECT
    DATA.value:id::string AS USER_ID,
    DATA.value:organisation_id::string AS ORGANISATION_ID,
    DATA.value:email::string AS EMAIL,
    DATA.value:firstname::string AS FIRST_NAME,
    DATA.value:lastname::string AS LAST_NAME,
    DATA.value:active::boolean AS ACTIVE,
    TO_CHAR(TO_TIMESTAMP_TZ(DATA.value:last_seen_at::string), 'YYYY-MM-DD') AS LAST_SEEN_AT,
    TO_CHAR(TO_TIMESTAMP_TZ(DATA_DATE), 'YYYY-MM-DD') AS EXPORTED_AT
FROM
    DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_USERS,
    LATERAL FLATTEN(input => JSON_PAYLOAD:data) AS DATA;
