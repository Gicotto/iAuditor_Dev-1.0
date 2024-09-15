CREATE OR REPLACE TABLE DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_TEMPLATES_FLTN AS
SELECT
    TO_CHAR(TO_TIMESTAMP_TZ(DATA_DATE), 'YYYY-MM-DD HH24:MI:SS') AS EXPORTED_AT,
    RECORD_INSERTED_AT,
    DATA.value:id::string AS TEMPLATE_ID,
    LEFT(DATA.value:archived::string, 1) AS ARCHIVED,
    DATA.value:name::string AS TEMPLATE_NAME,
    DATA.value:description::string AS DESCRIPTION,
    DATA.value:owner_name::string AS OWNER_NAME,
    DATA.value:owner_id::string AS OWNER_ID,
    DATA.value:author_name::string AS AUTHOR_NAME,
    DATA.value:author_id::string AS AUTHOR_ID,
    DATA.value:organisation_id::string AS ORGANISATION_ID,
    TO_CHAR(TO_TIMESTAMP_TZ(DATA.value:created_at::string), 'YYYY-MM-DD HH24:MI:SS') AS CREATED_AT,
    TO_CHAR(TO_TIMESTAMP_TZ(DATA.value:modified_at::string), 'YYYY-MM-DD HH24:MI:SS') AS MODIFIED_AT
    TO_CHAR(TO_TIMESTAMP_TZ(DATA.value:modified_at::string), 'YYYY-MM-DD HH24:MI:SS') AS MODIFIED_AT
FROM
    DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_TEMPLATES_FLTN,
    LATERAL FLATTEN(input => JSON_PAYLOAD:data) AS DATA;
