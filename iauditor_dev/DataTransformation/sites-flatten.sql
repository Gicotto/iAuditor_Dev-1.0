INSERT INTO DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_SITES_FLTN (
    SITE_ID,
    NAME,
    CREATOR_ID,
    ORGANISATION_ID,
    EXPORTED_AT,
    DELETED,
    SITE_UUID,
    META_LABEL,
    PARENT_ID
)
SELECT
    DATA.value:id::string AS SITE_ID,
    DATA.value:name::string AS NAME,
    DATA.value:creator_id::string AS CREATOR_ID,
    DATA.value:organisation_id::string AS ORGANISATION_ID,
    TO_CHAR(TO_TIMESTAMP_TZ(DATA_DATE), 'YYYY-MM-DD') AS EXPORTED_AT,
    DATA.value:deleted::boolean AS DELETED,
    DATA.value:site_uuid::string AS SITE_UUID,
    DATA.value:meta_label::string AS META_LABEL,
    DATA.value:parent_id::string AS PARENT_ID
FROM
    DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_SITES,
    LATERAL FLATTEN(input => JSON_PAYLOAD:data) AS DATA;
