CREATE OR REPLACE TABLE DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_ACTIONS_FLTN AS
SELECT
    DATA.value:id::string AS ACTION_ID,
    DATA.value:title::string AS TITLE,
    DATA.value:description::string AS DESCRIPTION,
    DATA.value:site_id::string AS SITE_ID,
    DATA.value:priority::string AS PRIORITY,
    DATA.value:status::string AS STATUS,
    TO_CHAR(TO_TIMESTAMP_TZ(DATA.value:due_date::string), 'YYYY-MM-DD') AS DUE_DATE,
    TO_CHAR(TO_TIMESTAMP_TZ(DATA.value:created_at::string), 'YYYY-MM-DD') AS CREATED_AT,
    TO_CHAR(TO_TIMESTAMP_TZ(DATA.value:modified_at::string), 'YYYY-MM-DD') AS MODIFIED_AT,
    TO_CHAR(TO_TIMESTAMP_TZ(DATA_DATE), 'YYYY-MM-DD') AS EXPORTED_AT,
    DATA.value:creator_user_id::string AS CREATOR_USER_ID,
    DATA.value:creator_user_name::string AS CREATOR_USER_NAME,
    DATA.value:template_id::string AS TEMPLATE_ID,
    DATA.value:audit_id::string AS AUDIT_ID,
    DATA.value:audit_title::string AS AUDIT_TITLE,
    DATA.value:audit_item_id::string AS AUDIT_ITEM_ID,
    DATA.value:audit_item_label::string AS AUDIT_ITEM_LABEL,
    DATA.value:organisation_id::string AS ORGANISATION_ID,
    TO_CHAR(TO_TIMESTAMP_TZ(DATA.value:completed_at::string), 'YYYY-MM-DD') AS COMPLETED_AT,
    DATA.value:action_label::string AS ACTION_LABEL,
    FALSE AS DELETED
    DATA.value:asset_id::string AS ASSET_ID
FROM
    DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_ACTIONS,
    LATERAL FLATTEN(input => JSON_PAYLOAD:data) AS DATA;
