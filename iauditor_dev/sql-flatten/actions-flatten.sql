CREATE OR REPLACE TABLE DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_ACTIONS_FLTN AS
SELECT
    DATA_DATE,
    RECORD_INSERTED_AT,
    DATA.value:id::string AS ACTION_ID,
    DATA.value:title::string AS TITLE,
    DATA.value:description::string AS DESCRIPTION,
    DATA.value:site_id::string AS SITE_ID,
    DATA.value:priority::string AS PRIORITY,
    DATA.value:status::string AS STATUS,
    DATA.value:due_date::string AS DUE_DATE,
    DATA.value:created_at::string AS CREATED_AT,
    DATA.value:modified_at::string AS MODIFIED_AT,
    DATA.value:creator_user_id::string AS CREATOR_USER_ID,
    DATA.value:creator_user_name::string AS ASSIGNEE_USER_NAME,
    DATA.value:template_id::string AS TEMPLATE_ID,
    DATA.value:audit_id::string AS AUDIT_ID,
    DATA.value:audit_title::string AS AUDIT_TITLE,
    DATA.value:audit_item_id::string AS AUDIT_ITEM_ID,
    DATA.value:audit_item_label::string AS AUDIT_ITEM_LABEL,
    DATA.value:organisation_id::string AS ORGANISATION_ID,
    DATA.value:completed_at::string AS COMPLETED_AT,
    DATA.value:action_label::string AS ACTION_LABEL,
    DATA.value:asset_id::string AS ASSET_ID
FROM
    DEV_RAW_DB.IAUDITOR_RAW.IAUDITOR_ACTIONS,
    LATERAL FLATTEN(input => JSON_PAYLOAD:data) AS DATA;
