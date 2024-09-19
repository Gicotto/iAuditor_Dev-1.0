INSERT INTO DEV_TRF_DB.IAUDITOR_TRF.IAUDITOR_USERS (
    USER_ID,
    ORGANISATION,
    EMAIL,
    FIRSTNAME,
    LASTNAME,
    ACTIVE,
    LAST_SEEN_AT,
    EXPORTED_AT
)
SELECT
    DATA.value:id::string AS USER_ID,
    DATA.value:organisation_id::string AS ORGANISATION,
    DATA.value:email::string AS EMAIL,
    DATA.value:firstname::string AS FIRSTNAME,
    DATA.value:lastname::string AS LASTNAME,
    DATA.value:active::boolean AS ACTIVE,
    TO_CHAR(TO_TIMESTAMP_TZ(DATA.value:last_seen_at::string), 'YYYY-MM-DD') AS LAST_SEEN_AT,
    TO_CHAR(TO_TIMESTAMP_TZ(DATA_DATE), 'YYYY-MM-DD') AS EXPORTED_AT
FROM
    DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_USERS,
    LATERAL FLATTEN(input => JSON_PAYLOAD:data) AS DATA;
