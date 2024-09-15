CREATE OR REPLACE TABLE DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_TEMPLATES_FLTN AS
SELECT
    DATA_DATE,
    RECORD_INSERTED_AT,
    DATA.value:id::string AS TEMPLATE_ID,
    DATA.value:archived::boolean AS ARCHIVED,
    DATA.value:name::string AS TEMPLATE_NAME,
    DATA.value:description::string AS DESCRIPTION,
    DATA.value:owner_name::string AS OWNER_NAME,
    DATA.value:owner_id::string AS OWNER_ID,
    DATA.value:author_name::string AS AUTHOR_NAME,
    DATA.value:author_id::string AS AUTHOR_ID,
    DATA.value:organisation_id::string AS ORGANISATION_ID,
    DATA.value:created_at::string AS CREATED_AT,
    DATA.value:modified_at::string AS MODIFIED_AT
FROM
    DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_TEMPLATES_FLTN,
    LATERAL FLATTEN(input => JSON_PAYLOAD:data) AS DATA;
