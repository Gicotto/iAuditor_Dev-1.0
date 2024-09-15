CREATE OR REPLACE TABLE DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_SITES_FLTN AS
SELECT
    DATA_DATE,
    RECORD_INSERTED_AT,
    DATA.value:id::string AS SITE_ID,
    DATA.value:name::string AS SITE_NAME,
    DATA.value:creator_id::string AS CREATOR_ID,
    DATA.value:organisation_id::string AS ORGANISATION_ID,
    DATA.value:deleted::boolean AS DELETED,
    DATA.value:site_uuid::string AS SITE_UUID,
    DATA.value:meta_label::string AS META_LABEL,
    DATA.value:parent_id::string AS PARENT_ID
FROM
    DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_SITES,
    LATERAL FLATTEN(input => JSON_PAYLOAD:data) AS DATA;
